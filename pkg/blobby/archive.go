package blobby

import (
	"context"
	"errors"
	"fmt"

	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/adammck/blobby/pkg/compactor"
	"github.com/adammck/blobby/pkg/memtable"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

type Blobby struct {
	mt    *memtable.Memtable
	bs    *blobstore.Blobstore
	md    *metadata.Store
	clock clockwork.Clock
	comp  *compactor.Compactor
}

func New(mongoURL, bucket string, clock clockwork.Clock) *Blobby {
	bs := blobstore.New(bucket, clock)
	md := metadata.New(mongoURL)

	return &Blobby{
		mt:    memtable.New(mongoURL, clock),
		bs:    bs,
		md:    md,
		clock: clock,
		comp:  compactor.New(bs, md, clock),
	}
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
		rec, bstats, err := b.bs.Find(ctx, meta.Filename(), key)
		if err != nil {
			return nil, stats, fmt.Errorf("blobstore.Get: %w", err)
		}

		// accumulate stats as we go
		stats.BlobsFetched++
		stats.RecordsScanned += bstats.RecordsScanned

		if rec != nil {
			// return as soon as we find the first record, but that's wrong!
			// before returning, we need to look at the record timestamp, and
			// check whether any of the remaining metas have a minTime newer
			// than that. this is only possible after a weird compaction.
			// TODO: fix this!
			stats.Source = bstats.Source
			return rec.Document, stats, nil
		}
	}

	// key not found
	return nil, stats, nil
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

	g.Go(func() error {
		var err error
		dest, _, meta, err = b.bs.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		return stats, err
	}

	// wait until the sstable is actually readable to update the stats.

	err = b.md.Insert(ctx, meta)
	if err != nil {
		// TODO: maybe delete the sstable(s) here, since they're orphaned.
		return stats, fmt.Errorf("metadata.Insert: %w", err)
	}

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
