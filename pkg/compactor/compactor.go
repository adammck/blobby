// pkg/compactor/compactor.go
package compactor

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
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

type CompactionOrder int

const (
	// OldestFirst considers files from oldest to newest, in terms of the
	// creation time, not the timestamps of the records it contains. This is
	// useful when old files are likely to contain data which can be expired.
	OldestFirst CompactionOrder = iota

	// NewestFirst considers files from newest to oldest. This is useful when
	// files are created rapidly, and should be compacted together regularly.
	// Should usually be combined with MinTime and/or MaxSize.
	NewestFirst

	// SmallestFirst considers files from smallest to largest. This is useful
	// when the overhead of having or touching many files is high, and we wish
	// to reduce the number of them.
	SmallestFirst

	// LargestFirst considers files from largest to smallest. Like OldestFirst,
	// this is most useful when looking for data to delete, or when scanning is
	// expensive and we want to repartition files.
	LargestFirst
)

type CompactionOptions struct {
	// Order specifies the order in which files should be considered for
	// compaction. The default is OldestFirst.
	Order CompactionOrder

	// MinTime specifies the minimum Timestamp of record which we want to
	// compact. Files containing only records older than this will be ignored.
	// Note that this does not affect time partioning of the output files.
	MinTime time.Time

	// MaxTime specifies the maximum Timestamp of record which we want to
	// compact. Files containing only records newer than this will be ignored.
	// Note that this does not affect time partioning of the output files.
	MaxTime time.Time

	// MinInputSize specifies the minimum total number of bytes which we will
	// compact. This is to avoid scheduling tiny compactions which are a waste
	// of time.
	MinInputSize int64

	// MaxInputSize specifies the maximum total number of bytes which we will
	// compact. This is to avoid scheduling huge compactions which take forever
	// or never complete.
	MaxInputSize int64

	// MinFiles specifies the minimum number of files which we will compact at
	// once. I'm not sure why this is here. Prefer MinInputSize.
	MinFiles int

	// MaxFiles specifies the maximum number of files which we will compact at
	// once. This is mostly to avoid shuffling too much metadata around.
	MaxFiles int

	// only compact a subset of the keyspace?
	//MinKey string
	//MaxKey string
}

type CompactionStats struct {
	Inputs  []*sstable.Meta
	Outputs []*sstable.Meta

	// Contains an error if the comnpaction failed.
	Error error
}

func (c *Compactor) Run(ctx context.Context, opts CompactionOptions) ([]*CompactionStats, error) {

	// grab *all* metadata for now, and do the selection in-process.
	// TODO: push down as much as we can to the mongo query.
	metas, err := c.md.GetAllMetas(ctx)
	if err != nil {
		return nil, fmt.Errorf("get metas: %w", err)
	}

	// get the list of blobs eligibile for compactions right now.
	compactions := c.GetCompactions(metas, opts)

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
				if err == io.EOF {
					break
				}
				return fmt.Errorf("NewMergeReader: %w", err)
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

func (c *Compactor) GetCompactions(metas []*sstable.Meta, opts CompactionOptions) []*Compaction {
	r := &Compaction{}
	var tot int64

	// return early if there aren't enough files to possibly qualify.
	if len(metas) < opts.MinFiles {
		return nil
	}

	// copy the param to avoid mutating during sort.
	smetas := make([]*sstable.Meta, len(metas))
	copy(smetas, metas)
	metas = nil

	// sort by whatever
	// TODO: move these into separate functions.
	sort.Slice(smetas, func(i, j int) bool {
		switch opts.Order {
		case OldestFirst:
			return smetas[i].Created.Before(smetas[j].Created)
		case NewestFirst:
			return smetas[j].Created.Before(smetas[i].Created)
		case SmallestFirst:
			return smetas[i].Size < smetas[j].Size
		case LargestFirst:
			return smetas[i].Size > smetas[j].Size
		default:
			panic(fmt.Sprintf("invalid sort order: %v", opts.Order))
		}
	})

	for _, m := range smetas {
		// skip if all records are before MinTime.
		if !opts.MinTime.IsZero() {
			if m.MaxTime.Before(opts.MinTime) {
				continue
			}
		}

		// skip if all records are after MaxTime.
		if !opts.MaxTime.IsZero() {
			if m.MinTime.After(opts.MaxTime) {
				continue
			}
		}

		// skip if adding this file would exceed the total cumulative input size
		// limit. (we don't know what the output file size would be, but it'll
		// be pretty similar because we're not expiring anything yet.)
		if opts.MaxInputSize != 0 {
			if tot+m.Size > opts.MaxInputSize {
				continue
			}
		}

		// skip if adding this file would add the input count limit.
		if opts.MaxFiles > 0 && len(r.Inputs) >= opts.MaxFiles {
			break
		}

		r.Inputs = append(r.Inputs, m)
		tot += m.Size
	}

	// abort if we don't have enough files left.
	if len(r.Inputs) < opts.MinFiles {
		return nil
	}

	// abort if the total input size is too small.
	if tot < opts.MinInputSize {
		return nil
	}

	return []*Compaction{r}
}
