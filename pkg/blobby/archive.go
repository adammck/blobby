package blobby

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/compactor"
	"github.com/adammck/blobby/pkg/filter"
	s3blobstore "github.com/adammck/blobby/pkg/impl/blobstore/s3"
	mfilterstore "github.com/adammck/blobby/pkg/impl/filterstore/mongo"
	mindexstore "github.com/adammck/blobby/pkg/impl/indexstore/mongo"
	"github.com/adammck/blobby/pkg/index"
	"github.com/adammck/blobby/pkg/iterator"
	"github.com/adammck/blobby/pkg/lru"
	"github.com/adammck/blobby/pkg/memtable"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	defaultDB         = "blobby"
	connectionTimeout = 3 * time.Second
	pingTimeout       = 3 * time.Second
	
	// LRU cache sizes - these prevent unbounded memory growth
	defaultIndexCacheSize  = 1000  // ~100MB assuming 100KB per index
	defaultFilterCacheSize = 10000 // ~10MB assuming 1KB per filter
	
	// Concurrency limits to prevent resource exhaustion
	defaultMaxConcurrentFlushes     = 2   // Allow 2 concurrent flushes
	defaultMaxConcurrentCompactions = 1   // Allow 1 concurrent compaction
	defaultMaxConcurrentScans       = 100 // Allow 100 concurrent scans
)

type Blobby struct {
	mt    *memtable.Memtable
	sstm  *sstable.Manager
	md    *metadata.Store
	ixs   api.IndexStore
	fs    api.FilterStore
	clock clockwork.Clock
	comp  *compactor.Compactor

	// LRU caches for indexes and filters
	indexCache  *lru.Cache
	filterCache *lru.Cache
	
	// Concurrency limits to prevent resource exhaustion
	flushSem   *semaphore.Weighted
	compactSem *semaphore.Weighted
	scanSem    *semaphore.Weighted
}

var _ api.Blobby = (*Blobby)(nil)

// scanResources manages the lifecycle of resources used during scan operations.
// This provides explicit cleanup semantics instead of relying on complex
// error-prone cleanup logic with boolean flags.
type scanResources struct {
	handles   []*memtable.Handle
	iterators []api.Iterator
}

// Close releases all resources, collecting any errors that occur.
// This method is safe to call multiple times.
func (sr *scanResources) Close() error {
	var errs []error
	
	// Close all iterators (refCountingIterator will handle releasing its own handle)
	for _, it := range sr.iterators {
		if it != nil {
			if err := it.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	sr.iterators = nil
	
	// Release any remaining handles not managed by iterators
	for _, h := range sr.handles {
		if h != nil {
			h.Release()
		}
	}
	sr.handles = nil
	
	// Return combined errors if any occurred
	if len(errs) > 0 {
		return fmt.Errorf("scan cleanup errors: %v", errs)
	}
	return nil
}

// addHandle adds a memtable handle to be managed
func (sr *scanResources) addHandle(handle *memtable.Handle) {
	sr.handles = append(sr.handles, handle)
}

// addIterator adds an iterator to be managed
func (sr *scanResources) addIterator(iter api.Iterator) {
	sr.iterators = append(sr.iterators, iter)
}

// resourceManagedIterator wraps an iterator and ensures all scan resources
// are properly cleaned up when the iterator is closed.
type resourceManagedIterator struct {
	api.Iterator
	resources *scanResources
	scanSem   *semaphore.Weighted // semaphore to release on close
}

// Close cleans up the underlying iterator and all associated resources
func (rmi *resourceManagedIterator) Close() error {
	var errs []error
	
	// Close the wrapped iterator first
	if err := rmi.Iterator.Close(); err != nil {
		errs = append(errs, err)
	}
	
	// Clean up all scan resources
	if err := rmi.resources.Close(); err != nil {
		errs = append(errs, err)
	}
	
	// Release scan semaphore
	if rmi.scanSem != nil {
		rmi.scanSem.Release(1)
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("resource cleanup errors: %v", errs)
	}
	return nil
}

func New(ctx context.Context, mongoURL, bucket string, clock clockwork.Clock, sf sstable.Factory) *Blobby {
	db, err := connectToMongo(ctx, mongoURL)
	if err != nil {
		// TODO: return error, obviously
		panic(fmt.Errorf("connectToMongo: %w", err))
	}

	ixs := mindexstore.New(db)
	fs := mfilterstore.New(db)
	md := metadata.New(mongoURL)
	sstm := sstable.NewManager(s3blobstore.New(bucket), clock, sf)

	return &Blobby{
		mt:    memtable.New(mongoURL, clock),
		sstm:  sstm,
		md:    md,
		ixs:   ixs,
		fs:    fs,
		clock: clock,
		comp:  compactor.New(clock, sstm, md, ixs, fs),

		indexCache:  lru.New(defaultIndexCacheSize),
		filterCache: lru.New(defaultFilterCacheSize),
		
		flushSem:   semaphore.NewWeighted(defaultMaxConcurrentFlushes),
		compactSem: semaphore.NewWeighted(defaultMaxConcurrentCompactions),
		scanSem:    semaphore.NewWeighted(defaultMaxConcurrentScans),
	}
}

func connectToMongo(ctx context.Context, mongoURL string) (*mongo.Database, error) {
	opt := options.Client().ApplyURI(mongoURL).SetTimeout(connectionTimeout)
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}

	ctxPing, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := client.Ping(ctxPing, nil); err != nil {
		return nil, err
	}

	return client.Database(defaultDB), nil
}

func (b *Blobby) Ping(ctx context.Context) error {
	err := b.mt.Ping(ctx)
	if err != nil {
		return fmt.Errorf("memtable.Ping: %w", err)
	}

	return nil
}

func (b *Blobby) Init(ctx context.Context) error {
	err := b.mt.Init(ctx)
	if err != nil {
		return fmt.Errorf("memtable.Init: %w", err)
	}

	err = b.md.Init(ctx)
	if err != nil {
		return fmt.Errorf("metadata.Init: %w", err)
	}

	return nil
}

func (b *Blobby) Put(ctx context.Context, key string, value []byte) (*api.PutStats, error) {
	start := b.clock.Now()
	stats := &api.PutStats{}
	
	rec := &types.Record{
		Key:      key,
		Document: value,
	}
	
	mtStats := &memtable.PutRecordStats{}
	dest, err := b.mt.PutRecordWithStats(ctx, rec, mtStats)
	if err != nil {
		return stats, err
	}
	
	stats.Destination = dest
	stats.WriteLatency = b.clock.Since(start)
	stats.RetryCount = mtStats.RetryCount
	
	return stats, nil
}

func (b *Blobby) Delete(ctx context.Context, key string) (*api.DeleteStats, error) {
	rec := &types.Record{
		Key:       key,
		Document:  nil,
		Tombstone: true,
	}

	dest, err := b.mt.PutRecord(ctx, rec)
	if err != nil {
		return nil, fmt.Errorf("memtable.PutRecord: %w", err)
	}

	return &api.DeleteStats{
		Timestamp:   rec.Timestamp,
		Destination: dest,
	}, nil
}

// TODO: return the Record, or maybe the timestamp too, not just the value.
func (b *Blobby) Get(ctx context.Context, key string) (value []byte, stats *api.GetStats, err error) {
	stats = &api.GetStats{}

	rec, src, err := b.mt.Get(ctx, key)
	if err != nil && !errors.Is(err, &memtable.NotFound{}) {
		return nil, stats, fmt.Errorf("memtable.Get: %w", err)
	}
	if rec != nil {
		stats.Source = src
		if rec.Tombstone {
			return nil, stats, &api.NotFound{Key: key}
		}
		return rec.Document, stats, nil
	}

	metas, err := b.md.GetContaining(ctx, key)
	if err != nil {
		return nil, stats, fmt.Errorf("metadata.GetContaining: %w", err)
	}

	// candidate holds a record and its source for comparison
	type candidate struct {
		rec    *types.Record
		source string
	}

	var best *candidate

	// note: this assumes that metas is already sorted.
	for _, meta := range metas {

		// check the (bloom) filter first, and skip the entire sstable if we can
		// confirm that no versions of the key are in it.
		f, err := b.getFilter(ctx, meta.Filename())
		if err != nil {
			// TODO: log and continue instead of returning.
			return nil, stats, fmt.Errorf("filter.Load: %w", err)
		}
		if !f.Contains(key) {
			stats.BlobsSkipped++
			continue
		}

		// fetch the index for this sstable. hopefully it's cached.
		idx, err := b.getIndex(ctx, meta.Filename())
		if err != nil {
			return nil, stats, fmt.Errorf("getIndex(%s): %w", key, err)
		}

		rng, err := idx.Lookup(key)
		if err != nil {
			// TODO: this need not be fatal. we can read the whole sstable.
			return nil, stats, fmt.Errorf("Index.Lookup(%s): %w", key, err)
		}

		var r *sstable.Reader
		if rng != nil {
			r, err = b.sstm.GetRange(ctx, meta.Filename(), rng.First, rng.Last)
			if err != nil {
				return nil, stats, fmt.Errorf("blobstore.GetPartial: %w", err)
			}
		} else {
			// if the index couldn't be fetched, that's not ideal, but we can
			// just read the entire sstable. hope it's not too big.
			r, err = b.sstm.GetFull(ctx, meta.Filename())
			if err != nil {
				return nil, stats, fmt.Errorf("blobstore.Get: %w", err)
			}
		}

		defer r.Close()

		var scanned int
		rec, scanned, err = b.ScanSSTable(ctx, r, key)
		if err != nil {
			return nil, stats, fmt.Errorf("Blobby.ScanSSTable: %w", err)
		}

		// accumulate stats as we go
		stats.BlobsFetched++
		stats.RecordsScanned += scanned

		if rec != nil {
			// Check if this is the newest version we've seen
			if best == nil || rec.Timestamp.After(best.rec.Timestamp) {
				best = &candidate{rec: rec, source: meta.Filename()}
			}
		}
	}

	// Return the newest version found
	if best != nil {
		stats.Source = best.source
		if best.rec.Tombstone {
			return nil, stats, &api.NotFound{Key: key}
		}
		return best.rec.Document, stats, nil
	}

	// key not found
	return nil, stats, &api.NotFound{Key: key}
}

// getIndex returns the index for the given sstable. If the index is not already
// cached, it will be fetched from the index store and cached in an LRU cache.
func (b *Blobby) getIndex(ctx context.Context, fn string) (*index.Index, error) {
	// Check cache first
	if cached, found := b.indexCache.Get(fn); found {
		return cached.(*index.Index), nil
	}

	// Load from store
	ixs, err := b.ixs.Get(ctx, fn)
	if err != nil && !errors.Is(err, &api.IndexNotFound{}) {
		return nil, fmt.Errorf("IndexStore.Get(%s): %w", fn, err)
	}

	ix := index.New(ixs)
	b.indexCache.Put(fn, ix)
	return ix, nil
}

// getFilter returns the filter for the given sstable. If it's not already in
// the cache, it will be fetched from the FilterStore and cached in an LRU cache.
func (b *Blobby) getFilter(ctx context.Context, fn string) (filter.Filter, error) {
	// Check cache first
	if cached, found := b.filterCache.Get(fn); found {
		return cached.(filter.Filter), nil
	}

	// Load from store
	fi, err := b.fs.Get(ctx, fn)
	if err != nil {
		return nil, err
	}

	f, err := filter.Unmarshal(fi)
	if err != nil {
		return nil, err
	}

	b.filterCache.Put(fn, f)
	return f, nil
}

// ScanSSTable reads from the given sstable reader until it finds a record with the
// given key. If EOF is reached, nil is returned. For efficiency, the reader
// should already be *near* the record by using an index.
func (b *Blobby) ScanSSTable(ctx context.Context, reader *sstable.Reader, key string) (*types.Record, int, error) {
	var rec *types.Record
	var scanned int
	var err error

	for {
		rec, err = reader.Next()
		if err != nil {
			return nil, scanned, fmt.Errorf("sstable.Reader.Next: %w", err)
		}
		if rec == nil {
			// end of file
			return nil, scanned, nil
		}

		scanned++

		if rec.Key == key {
			break
		}
	}

	return rec, scanned, nil
}

// Scan returns an iterator for all keys in the range [start, end).
// If end is empty, the scan is unbounded (all keys >= start).
// The returned iterator must be closed to avoid resource leaks.
func (b *Blobby) Scan(ctx context.Context, start, end string) (api.Iterator, *api.ScanStats, error) {
	// Acquire scan semaphore to limit concurrent scans
	if err := b.scanSem.Acquire(ctx, 1); err != nil {
		return nil, nil, fmt.Errorf("acquire scan semaphore: %w", err)
	}
	// Note: semaphore will be released when iterator is closed
	
	stats := &api.ScanStats{}

	// validate range
	if start > end && end != "" {
		b.scanSem.Release(1) // Release semaphore on validation failure
		return nil, stats, fmt.Errorf("invalid range: start=%q > end=%q", start, end)
	}

	// Use explicit resource management instead of fragile cleanup logic
	resources := &scanResources{}
	var transferredOwnership bool
	defer func() {
		// Clean up resources if we haven't transferred ownership
		if !transferredOwnership {
			b.scanSem.Release(1) // Release semaphore on early return
			resources.Close()
		}
	}()

	// collect memtable iterators with reference counting (newest first)
	memtables, err := b.mt.ListMemtables(ctx)
	if err != nil {
		return nil, stats, fmt.Errorf("ListMemtables: %w", err)
	}

	for _, name := range memtables {
		// get handle and add reference to prevent deletion during scan
		handle, err := b.mt.GetHandle(ctx, name)
		if err != nil {
			return nil, stats, fmt.Errorf("GetHandle(%s): %w", name, err)
		}

		handle.AddRef()

		iter, err := b.mt.Scan(ctx, name, start, end)
		if err != nil {
			return nil, stats, fmt.Errorf("memtable.Scan(%s): %w", name, err)
		}

		// wrap iterator to handle reference counting on close
		refIter := &refCountingIterator{
			Iterator: iter,
			handle:   handle,
		}

		resources.addIterator(refIter)
		stats.MemtablesRead++
	}

	// collect sstable iterators (by metadata ordering)
	metas, err := b.md.GetAllMetas(ctx)
	if err != nil {
		return nil, stats, fmt.Errorf("metadata.GetAllMetas: %w", err)
	}

	for _, meta := range metas {
		// skip sstables that can't contain keys in our range
		if end != "" && meta.MinKey >= end {
			continue
		}
		if start != "" && meta.MaxKey < start {
			continue
		}

		reader, err := b.sstm.GetFull(ctx, meta.Filename())
		if err != nil {
			return nil, stats, fmt.Errorf("sstm.GetFull(%s): %w", meta.Filename(), err)
		}

		iter := sstable.NewRangeIterator(reader, start, end)
		resources.addIterator(iter)
		stats.SstablesRead++
	}

	compound := iterator.New(ctx, resources.iterators)
	counting := iterator.NewCounting(compound, stats)

	// Create a cleanup iterator that manages all our resources
	cleanupIter := &resourceManagedIterator{
		Iterator:  counting,
		resources: resources,
		scanSem:   b.scanSem,
	}

	// Transfer ownership - prevent defer cleanup
	transferredOwnership = true

	return cleanupIter, stats, nil
}


// refCountingIterator wraps a memtable iterator and manages reference counting
// to prevent memtables from being dropped while scans are active.
//
// This is needed because:
// 1. Memtables can be dropped during compaction when their reference count reaches zero
// 2. Long-running scans need to ensure the underlying collection doesn't get deleted
// 3. Handle.AddRef()/Release() provides the reference counting, but we need to tie
//    iterator lifecycle to handle lifecycle 
// 4. When the iterator is closed, we Release() the handle to allow cleanup
//
// Without this wrapper, a memtable could be dropped mid-scan, causing the 
// iterator to fail with "collection does not exist" errors.
type refCountingIterator struct {
	api.Iterator
	handle *memtable.Handle
}

func (r *refCountingIterator) Close() error {
	err := r.Iterator.Close()
	r.handle.Release()
	return err
}

func (b *Blobby) Flush(ctx context.Context) (*api.FlushStats, error) {
	// Acquire flush semaphore to limit concurrent flushes
	if err := b.flushSem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("acquire flush semaphore: %w", err)
	}
	defer b.flushSem.Release(1)
	
	stats := &api.FlushStats{}

	// TODO: check whether old sstable is still flushing
	hPrev, hNext, err := b.mt.Rotate(ctx)
	if err != nil {
		return stats, fmt.Errorf("memtable.Swap: %w", err)
	}

	stats.ActiveMemtable = hNext.Name()

	ch := make(chan *types.Record)
	g, ctx2 := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := hPrev.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("memtable.Flush: %w", err)
		}
		return nil
	})

	var meta *api.BlobMeta
	var idx []api.IndexEntry
	var f filter.Filter

	g.Go(func() error {
		var err error
		meta, idx, f, err = b.sstm.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		return stats, err
	}

	err = b.md.Insert(ctx, meta)
	if err != nil {
		// TODO: maybe delete the sstable(s) here, since they're orphaned.
		return stats, fmt.Errorf("metadata.Insert: %w", err)
	}

	err = b.ixs.Put(ctx, meta.Filename(), idx)
	if err != nil {
		// TODO: roll back metadata insert
		return stats, fmt.Errorf("IndexStore.Put: %w", err)
	}

	// marshal the filter here. not sure why!
	fi, err := f.Marshal()
	if err != nil {
		// TODO: roll back everything, or maybe do this above.
		return stats, fmt.Errorf("Filter.Marshal: %w", err)
	}

	// write the filter using a filterstore
	err = b.fs.Put(ctx, meta.Filename(), fi)
	if err != nil {
		// TODO: roll back metadata insert
		return stats, fmt.Errorf("FilterStore.Put: %w", err)
	}

	// wait until the sstable is actually readable to update the stats.
	stats.FlushedMemtable = hPrev.Name()
	stats.Meta = meta

	// atomically drop memtable if no active scans are using it
	err = b.mt.TryDrop(ctx, hPrev.Name())
	if err != nil {
		if errors.Is(err, memtable.ErrStillReferenced) {
			// Memtable still has active references, skip dropping for now
			// TODO: if we can't drop it now, we should retry later when refs reach zero
			return stats, nil
		}
		return stats, fmt.Errorf("memtable.TryDrop: %w", err)
	}

	return stats, nil
}

func (b *Blobby) Compact(ctx context.Context, opts api.CompactionOptions) ([]*api.CompactionStats, error) {
	// Acquire compaction semaphore to limit concurrent compactions
	if err := b.compactSem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("acquire compaction semaphore: %w", err)
	}
	defer b.compactSem.Release(1)
	
	return b.comp.Run(ctx, opts)
}

