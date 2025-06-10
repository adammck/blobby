package blobby

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/compactor"
	"github.com/adammck/blobby/pkg/filter"
	s3blobstore "github.com/adammck/blobby/pkg/impl/blobstore/s3"
	mfilterstore "github.com/adammck/blobby/pkg/impl/filterstore/mongo"
	mindexstore "github.com/adammck/blobby/pkg/impl/indexstore/mongo"
	"github.com/adammck/blobby/pkg/index"
	"github.com/adammck/blobby/pkg/iterator"
	"github.com/adammck/blobby/pkg/memtable"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	defaultDB         = "blobby"
	connectionTimeout = 3 * time.Second
	pingTimeout       = 3 * time.Second
)

type Blobby struct {
	mt    *memtable.Memtable
	sstm  *sstable.Manager
	md    *metadata.Store
	ixs   api.IndexStore
	fs    api.FilterStore
	clock clockwork.Clock
	comp  *compactor.Compactor

	// index cache
	indexesMu sync.Mutex
	indexes   map[string]*index.Index

	// filter cache
	filtersMu sync.Mutex
	filters   map[string]filter.Filter
}

var _ api.Blobby = (*Blobby)(nil)

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

		indexes: map[string]*index.Index{},
		filters: map[string]filter.Filter{},
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

func (b *Blobby) Put(ctx context.Context, key string, value []byte) (string, error) {
	return b.mt.Put(ctx, key, value)
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
		rec, scanned, err = b.Scan(ctx, r, key)
		if err != nil {
			return nil, stats, fmt.Errorf("Blobby.Scan: %w", err)
		}

		// accumulate stats as we go
		stats.BlobsFetched++
		stats.RecordsScanned += scanned

		if rec != nil {
			stats.Source = meta.Filename()
			if rec.Tombstone {
				return nil, stats, &api.NotFound{Key: key}
			}
			// return as soon as we find the first record, but that's wrong!
			// before returning, we need to look at the record timestamp, and
			// check whether any of the remaining metas have a minTime newer
			// than that. this is only possible after a weird compaction.
			// TODO: fix this!
			return rec.Document, stats, nil
		}
	}

	// key not found
	return nil, stats, &api.NotFound{Key: key}
}

// getIndex returns the index for the given sstable. If the index is not already
// cached, it will be fetched from the index store and cached forever.
//
// TODO: add some kind of expiration policy.
func (b *Blobby) getIndex(ctx context.Context, fn string) (*index.Index, error) {
	b.indexesMu.Lock()
	defer b.indexesMu.Unlock()

	if ix, ok := b.indexes[fn]; ok {
		return ix, nil
	}

	ixs, err := b.ixs.Get(ctx, fn)
	if err != nil && !errors.Is(err, &api.IndexNotFound{}) {
		return nil, fmt.Errorf("IndexStore.Get(%s): %w", fn, err)
	}

	ix := index.New(ixs)
	b.indexes[fn] = ix
	return ix, nil
}

// getFilter returns the filter for the given sstable. If it's not already in
// the cache, it will be fetched from the FilterStore and cached forever.
//
// TODO: add some kind of expiration policy.
func (b *Blobby) getFilter(ctx context.Context, fn string) (filter.Filter, error) {
	b.filtersMu.Lock()
	defer b.filtersMu.Unlock()

	if f, ok := b.filters[fn]; ok {
		return f, nil
	}

	fi, err := b.fs.Get(ctx, fn)
	if err != nil {
		return nil, err
	}

	f, err := filter.Unmarshal(fi)
	if err != nil {
		return nil, err
	}

	b.filters[fn] = f
	return f, nil
}

// Scan reads from the given sstable reader until it finds a record with the
// given key. If EOF is reached, nil is returned. For efficiency, the reader
// should already be *near* the record by using an index.
func (b *Blobby) Scan(ctx context.Context, reader *sstable.Reader, key string) (*types.Record, int, error) {
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

// RangeScan returns an iterator for all keys in the range [start, end)
func (b *Blobby) RangeScan(ctx context.Context, start, end string) (api.Iterator, *api.ScanStats, error) {
	stats := &api.ScanStats{}

	// validate range
	if start > end && end != "" {
		return nil, stats, fmt.Errorf("invalid range: start=%q > end=%q", start, end)
	}

	var iterators []api.Iterator
	var sources []string

	// collect memtable iterators (newest first)
	memtables, err := b.mt.ListMemtables(ctx)
	if err != nil {
		return nil, stats, fmt.Errorf("ListMemtables: %w", err)
	}

	for _, name := range memtables {
		iter, err := b.mt.Scan(ctx, name, start, end)
		if err != nil {
			// cleanup already created iterators
			for _, it := range iterators {
				it.Close()
			}
			return nil, stats, fmt.Errorf("memtable.Scan(%s): %w", name, err)
		}
		iterators = append(iterators, iter)
		sources = append(sources, "memtable:"+name)
	}

	// collect sstable iterators (by metadata ordering)
	metas, err := b.md.GetAllMetas(ctx)
	if err != nil {
		// cleanup memtable iterators
		for _, it := range iterators {
			it.Close()
		}
		return nil, stats, fmt.Errorf("metadata.GetAllMetas: %w", err)
	}

	for _, meta := range metas {
		reader, err := b.sstm.GetFull(ctx, meta.Filename())
		if err != nil {
			// cleanup already created iterators
			for _, it := range iterators {
				it.Close()
			}
			return nil, stats, fmt.Errorf("sstm.GetFull(%s): %w", meta.Filename(), err)
		}

		iter := sstable.NewRangeIterator(reader, start, end)
		iterators = append(iterators, iter)
		sources = append(sources, "sstable:"+meta.Filename())
	}

	// create compound iterator
	compound := iterator.New(ctx, iterators, sources)

	// wrap in counting iterator to track RecordsReturned
	counting := &countingIterator{
		inner: compound,
		stats: stats,
	}

	return counting, stats, nil
}

// countingIterator wraps another iterator and tracks the number of records returned
type countingIterator struct {
	inner api.Iterator
	stats *api.ScanStats
}

func (c *countingIterator) Next(ctx context.Context) bool {
	hasNext := c.inner.Next(ctx)
	if hasNext {
		c.stats.RecordsReturned++
	}
	return hasNext
}

func (c *countingIterator) Key() string {
	return c.inner.Key()
}

func (c *countingIterator) Value() []byte {
	return c.inner.Value()
}

func (c *countingIterator) Timestamp() time.Time {
	return c.inner.Timestamp()
}

func (c *countingIterator) Err() error {
	return c.inner.Err()
}

func (c *countingIterator) Close() error {
	return c.inner.Close()
}

func (b *Blobby) Flush(ctx context.Context) (*api.FlushStats, error) {
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

	err = b.mt.Drop(ctx, hPrev.Name())
	if err != nil {
		return stats, fmt.Errorf("memtable.Drop: %w", err)
	}

	return stats, nil
}

func (b *Blobby) Compact(ctx context.Context, opts api.CompactionOptions) ([]*api.CompactionStats, error) {
	return b.comp.Run(ctx, opts)
}
