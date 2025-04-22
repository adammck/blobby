// pkg/compactor/compactor.go
package compactor

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

type Compactor struct {
	bs    *sstable.Manager
	md    *metadata.Store
	ixs   api.IndexStore
	fs    api.FilterStore
	clock clockwork.Clock
}

func New(clock clockwork.Clock, bs *sstable.Manager, md *metadata.Store, ixs api.IndexStore, fs api.FilterStore) *Compactor {
	return &Compactor{
		clock: clock,
		bs:    bs,
		md:    md,
		ixs:   ixs,
		fs:    fs,
	}
}

func (c *Compactor) Run(ctx context.Context, opts api.CompactionOptions) ([]*api.CompactionStats, error) {

	// grab *all* metadata for now, and do the selection in-process.
	// TODO: push down as much as we can to the mongo query.
	metas, err := c.md.GetAllMetas(ctx)
	if err != nil {
		return nil, fmt.Errorf("get metas: %w", err)
	}

	// get the list of blobs eligibile for compactions right now.
	compactions := c.GetCompactions(metas, opts)

	stats := []*api.CompactionStats{}
	for _, cc := range compactions {
		s := c.Compact(ctx, cc)
		stats = append(stats, s)
	}

	return stats, nil
}

func (c *Compactor) Compact(ctx context.Context, cc *Compaction) *api.CompactionStats {
	stats := &api.CompactionStats{
		Inputs: cc.Inputs,
	}

	readers := make([]*sstable.Reader, len(cc.Inputs))
	for i, m := range cc.Inputs {
		r, err := c.bs.GetFull(ctx, m.Filename())
		if err != nil {
			stats.Error = fmt.Errorf("getSST(%s): %w", m.Filename(), err)
			return stats
		}
		defer r.Close()
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

	var meta *api.BlobMeta
	var idx []api.IndexEntry
	var f filter.Filter

	g.Go(func() error {
		var err error
		meta, idx, f, err = c.bs.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err := g.Wait()
	if err != nil {
		return &api.CompactionStats{
			Error: fmt.Errorf("g.Wait: %w", err),
		}
	}

	stats.Outputs = []*api.BlobMeta{meta}

	// TODO: Do the inserts and deletes transactionally!

	err = c.md.Insert(ctx, meta)
	if err != nil {
		return &api.CompactionStats{
			Error: fmt.Errorf("metadata.Insert: %w", err),
		}
	}

	err = c.ixs.Put(ctx, meta.Filename(), idx)
	if err != nil {
		return &api.CompactionStats{
			Error: fmt.Errorf("IndexStore.Put: %w", err),
		}
	}

	// marshal the filter here. not sure why!
	fi, err := f.Marshal()
	if err != nil {
		// TODO: roll back everything, or maybe do this above.
		return &api.CompactionStats{
			Error: fmt.Errorf("Filter.Marshal: %w", err),
		}
	}

	// write the filter using a filterstore
	err = c.fs.Put(ctx, meta.Filename(), fi)
	if err != nil {
		// TODO: roll back metadata insert
		return &api.CompactionStats{
			Error: fmt.Errorf("FilterStore.Put: %w", err),
		}
	}

	// delete the input files from the metadata store, so they're no longer
	// returned for queries, before removing the actual files.

	for i, m := range cc.Inputs {
		err = c.md.Delete(ctx, m)
		if err != nil {
			return &api.CompactionStats{
				// TODO: include the metadata ID in this error.
				Error: fmt.Errorf("metadata.Delete(%d): %w", i, err),
			}
		}
	}

	// delete the blobs.

	for _, m := range cc.Inputs {
		c.bs.Delete(ctx, m.Filename())
		if err != nil {
			return &api.CompactionStats{
				Error: fmt.Errorf("blobstore.Delete(%s): %w", m.Filename(), err),
			}
		}
	}

	return stats
}

type Compaction struct {
	Inputs []*api.BlobMeta
}

func (c *Compactor) GetCompactions(metas []*api.BlobMeta, opts api.CompactionOptions) []*Compaction {
	r := &Compaction{}
	var tot int64

	// return early if there aren't enough files to possibly qualify.
	if len(metas) < opts.MinFiles {
		return nil
	}

	// copy the param to avoid mutating during sort.
	smetas := make([]*api.BlobMeta, len(metas))
	copy(smetas, metas)
	metas = nil

	// sort by whatever
	// TODO: move these into separate functions.
	sort.Slice(smetas, func(i, j int) bool {
		switch opts.Order {
		case api.OldestFirst:
			return smetas[i].Created.Before(smetas[j].Created)
		case api.NewestFirst:
			return smetas[j].Created.Before(smetas[i].Created)
		case api.SmallestFirst:
			return smetas[i].Size < smetas[j].Size
		case api.LargestFirst:
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
