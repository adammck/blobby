// pkg/compactor/compactor.go
package compactor

import (
	"context"
	"fmt"

	"github.com/adammck/archive/pkg/blobstore"
	"github.com/adammck/archive/pkg/metadata"
	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/types"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

type Compactor struct {
	bs    *blobstore.Blobstore
	md    *metadata.Store
	clock clockwork.Clock
}

func New(bs *blobstore.Blobstore, md *metadata.Store, clock clockwork.Clock) *Compactor {
	return &Compactor{
		bs:    bs,
		md:    md,
		clock: clock,
	}
}

type CompactionStats struct {
	Inputs  []*sstable.Meta
	Outputs []*sstable.Meta

	// Contains an error if the comnpaction failed.
	Error error
}

func (c *Compactor) Run(ctx context.Context) ([]*CompactionStats, error) {

	// grab *all* metadata for now, and do the selection in-process.
	// TODO: push down as much as we can to the mongo query.
	metas, err := c.md.GetAllMetas(ctx)
	if err != nil {
		return nil, fmt.Errorf("get metas: %w", err)
	}

	// get the list of blobs eligibile for compactions right now.
	compactions := c.GetCompactions(metas)

	stats := []*CompactionStats{}
	for _, cc := range compactions {
		s := c.Compact(ctx, cc)
		stats = append(stats, s)
	}

	return stats, nil
}

func (c *Compactor) Compact(ctx context.Context, cc *Compaction) *CompactionStats {
	stats := &CompactionStats{
		Inputs: cc.Inputs,
	}

	readers := make([]*sstable.Reader, len(cc.Inputs))
	for i, m := range cc.Inputs {
		r, err := c.bs.Get(ctx, m.Filename())
		if err != nil {
			stats.Error = fmt.Errorf("getSST(%s): %w", m.Filename(), err)
			return stats
		}
		readers[i] = r
	}

	// TODO: do filtering here, to expire data by timestamp and version count

	// TODO: also do partitioning here, so large files can be split by key.

	ch := make(chan *types.Record)
	g, ctx2 := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(ch)

		mr, err := sstable.NewMergeReader(readers)
		if err != nil {
			return fmt.Errorf("NewMergeReader: %w", err)
		}

		for {
			rec, err := mr.Next()
			if err != nil {
				return fmt.Errorf("NewMergeReader: %w", err)
			}
			if rec == nil { // EOF
				break
			}
			ch <- rec
		}

		return nil
	})

	var meta *sstable.Meta

	g.Go(func() error {
		var err error
		_, _, meta, err = c.bs.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err := g.Wait()
	if err != nil {
		return &CompactionStats{
			Error: fmt.Errorf("g.Wait: %w", err),
		}
	}

	stats.Outputs = []*sstable.Meta{meta}

	// TODO: Do the inserts and deletes transactionally!

	err = c.md.Insert(ctx, meta)
	if err != nil {
		return &CompactionStats{
			Error: fmt.Errorf("metadata.Insert: %w", err),
		}
	}

	// delete the input files from the metadata store, so they're no longer
	// returned for queries, before removing the actual files.

	for i, m := range cc.Inputs {
		err = c.md.Delete(ctx, m)
		if err != nil {
			return &CompactionStats{
				// TODO: include the metadata ID in this error.
				Error: fmt.Errorf("metadata.Delete(%d): %w", i, err),
			}
		}
	}

	// delete the blobs.

	for _, m := range cc.Inputs {
		c.bs.Delete(ctx, m.Filename())
		if err != nil {
			return &CompactionStats{
				Error: fmt.Errorf("blobstore.Delete(%s): %w", m.Filename(), err),
			}
		}
	}

	return stats
}

type Compaction struct {
	Inputs []*sstable.Meta
}

// for now, just compact together all files into a single file.
func (c *Compactor) GetCompactions(metas []*sstable.Meta) []*Compaction {
	r := &Compaction{}

	for _, m := range metas {
		r.Inputs = append(r.Inputs, m)
	}

	return []*Compaction{
		r,
	}
}
