package blobby

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/adammck/blobby/pkg/compactor"
	mongoindex "github.com/adammck/blobby/pkg/impl/index/mongo"
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
	bs    *blobstore.Blobstore
	md    *metadata.Store
	ixs   api.IndexStore
	clock clockwork.Clock
	comp  *compactor.Compactor

	// Options for the SSTable writer.
	fopts []sstable.WriterOption
}

func New(ctx context.Context, mongoURL, bucket string, clock clockwork.Clock) *Blobby {
	db, err := connectToMongo(ctx, mongoURL)
	if err != nil {
		// TODO: return error, obviously
		panic(fmt.Errorf("connectToMongo: %w", err))
	}

	ixs := mongoindex.New(db)
	bs := blobstore.New(bucket, clock)
	md := metadata.New(mongoURL)

	// TODO: make this configurable
	// TODO: also use more sensible defaults
	fopts := []sstable.WriterOption{
		sstable.WithIndexEveryNRecords(32),
	}

	return &Blobby{
		mt:    memtable.New(mongoURL, clock),
		bs:    bs,
		md:    md,
		ixs:   ixs,
		clock: clock,
		comp:  compactor.New(clock, bs, md, ixs, fopts),
		fopts: fopts,
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

	err = b.bs.Ping(ctx)
	if err != nil {
		return fmt.Errorf("blobstore.Ping: %w", err)
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

type GetStats struct {
	Source         string
	BlobsFetched   int
	RecordsScanned int
}

// TODO: return the Record, or maybe the timestamp too, not just the value.
func (b *Blobby) Get(ctx context.Context, key string) (value []byte, stats *GetStats, err error) {
	stats = &GetStats{}

	rec, src, err := b.mt.Get(ctx, key)
	if err != nil && !errors.Is(err, &memtable.NotFound{}) {
		return nil, stats, fmt.Errorf("memtable.Get: %w", err)
	}
	if rec != nil {
		// TODO: Update Memtable.Get to return stats too.
		stats.Source = src
		return rec.Document, stats, nil
	}

	metas, err := b.md.GetContaining(ctx, key)
	if err != nil {
		return nil, stats, fmt.Errorf("metadata.GetContaining: %w", err)
	}

	// note: this assumes that metas is already sorted.
	for _, meta := range metas {

		// fetch the index for this sstable.
		ixs, err := b.ixs.Get(ctx, meta.Filename())
		if err != nil {
			if !errors.Is(err, &api.IndexNotFound{}) {
				return nil, stats, fmt.Errorf("IndexStore.Get(%s): %w", meta.Filename(), err)
			}
		}

		var r *sstable.Reader
		var first, last int64

		// find the byte range we need to look at for this key.
		// TODO: extract this into a func so we can test it.
		// TODO: cache the index in-process; it's immutable.
		// TODO: materialize the index, so we can use binary search.
		for i := range ixs {
			if ixs[i].Key < key {
				first = ixs[i].Offset
			}
			if ixs[i].Key > key {
				// Offset is the first byte of the next segment, and byte range
				// fetch is inclusive, so stop before it.
				last = ixs[i].Offset - 1
				break
			}
		}

		if first > 0 {
			r, err = b.bs.GetPartial(ctx, meta.Filename(), first, last)
			if err != nil {
				return nil, stats, fmt.Errorf("blobstore.GetPartial: %w", err)
			}
		} else {
			// if the index couldn't be fetched, that's not ideal, but we can
			// just read the entire sstable. hope it's not too big.
			r, err = b.bs.GetFull(ctx, meta.Filename())
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
			// return as soon as we find the first record, but that's wrong!
			// before returning, we need to look at the record timestamp, and
			// check whether any of the remaining metas have a minTime newer
			// than that. this is only possible after a weird compaction.
			// TODO: fix this!
			stats.Source = meta.Filename()
			return rec.Document, stats, nil
		}
	}

	// key not found
	return nil, stats, nil
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

type FlushStats struct {

	// The URL of the memtable which was flushed.
	FlushedMemtable string

	// The name of the memtable that is now active, after the flush.
	ActiveMemtable string

	// The key of the flushed sstable in the blobstore.
	BlobName string

	// Metadata about the flushed sstable.
	Meta *sstable.Meta
}

func (b *Blobby) Flush(ctx context.Context) (*FlushStats, error) {
	stats := &FlushStats{}

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

	var dest string
	var meta *sstable.Meta
	var idx api.Index

	g.Go(func() error {
		var err error
		dest, _, meta, idx, err = b.bs.Flush(ctx2, ch, b.fopts...)
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

	// wait until the sstable is actually readable to update the stats.
	stats.FlushedMemtable = hPrev.Name()
	stats.BlobName = dest
	stats.Meta = meta

	err = b.mt.Drop(ctx, hPrev.Name())
	if err != nil {
		return stats, fmt.Errorf("memtable.Drop: %w", err)
	}

	return stats, nil
}

type CompactionStats = compactor.CompactionStats
type CompactionOptions = compactor.CompactionOptions

func (b *Blobby) Compact(ctx context.Context, opts CompactionOptions) ([]*CompactionStats, error) {
	return b.comp.Run(ctx, opts)
}
