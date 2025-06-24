// pkg/compactor/compactor.go
package compactor

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter"
	"github.com/adammck/blobby/pkg/metadata"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

type Compactor struct {
	sstm  *sstable.Manager
	md    *metadata.Store
	ixs   api.IndexStore
	fs    api.FilterStore
	clock clockwork.Clock
}

func New(clock clockwork.Clock, sstm *sstable.Manager, md *metadata.Store, ixs api.IndexStore, fs api.FilterStore) *Compactor {
	return &Compactor{
		clock: clock,
		sstm:  sstm,
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
		s := c.Compact(ctx, cc, opts.GC)
		stats = append(stats, s)
	}

	return stats, nil
}

func (c *Compactor) Compact(ctx context.Context, cc *Compaction, gcPolicy *api.GCPolicy) *api.CompactionStats {
	stats := &api.CompactionStats{
		Inputs: cc.Inputs,
	}

	readers := make([]*sstable.Reader, len(cc.Inputs))
	for i, m := range cc.Inputs {
		r, err := c.sstm.GetFull(ctx, m.Filename())
		if err != nil {
			stats.Error = fmt.Errorf("getSST(%s): %w", m.Filename(), err)
			return stats
		}
		defer r.Close()
		readers[i] = r
	}

	ch := make(chan *types.Record)
	g, ctx2 := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(ch)

		mr, err := sstable.NewMergeReader(readers)
		if err != nil {
			return fmt.Errorf("NewMergeReader: %w", err)
		}

		// Apply GC policy if specified
		if gcPolicy != nil {
			return c.mergeWithGC(ctx2, mr, ch, gcPolicy)
		} else {
			return c.mergeWithoutGC(ctx2, mr, ch)
		}
	})

	var meta *api.BlobMeta
	var idx []api.IndexEntry
	var f filter.Filter

	g.Go(func() error {
		var err error
		meta, idx, f, err = c.sstm.Flush(ctx2, ch)
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

	// Use atomic compaction to ensure consistency
	return c.compactWithRollback(ctx, cc, meta, idx, f)
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

// compactWithRollback performs atomic compaction with rollback capability
func (c *Compactor) compactWithRollback(ctx context.Context, cc *Compaction, meta *api.BlobMeta, idx []api.IndexEntry, f filter.Filter) *api.CompactionStats {
	// Phase 1: Prepare all data (can be retried if fails)
	fi, err := f.Marshal()
	if err != nil {
		return &api.CompactionStats{
			Error: fmt.Errorf("Filter.Marshal: %w", err),
		}
	}

	// Phase 2: Write index and filter (idempotent operations)
	err = c.ixs.Put(ctx, meta.Filename(), idx)
	if err != nil {
		return &api.CompactionStats{
			Error: fmt.Errorf("IndexStore.Put: %w", err),
		}
	}

	err = c.fs.Put(ctx, meta.Filename(), fi)
	if err != nil {
		// Clean up index
		c.ixs.Delete(ctx, meta.Filename()) // Best effort cleanup
		return &api.CompactionStats{
			Error: fmt.Errorf("FilterStore.Put: %w", err),
		}
	}

	// Phase 3: Atomic metadata swap using MongoDB transaction
	err = c.md.AtomicSwap(ctx, meta, cc.Inputs)
	if err != nil {
		// Rollback: clean up index and filter
		c.ixs.Delete(ctx, meta.Filename()) // Best effort cleanup
		c.fs.Delete(ctx, meta.Filename()) // Best effort cleanup
		return &api.CompactionStats{
			Error: fmt.Errorf("atomic metadata swap: %w", err),
		}
	}

	// Phase 4: Clean up old blobs (safe to fail - metadata already updated)
	for _, m := range cc.Inputs {
		err := c.sstm.Delete(ctx, m.Filename())
		if err != nil {
			// Log error but don't fail compaction - metadata is already consistent
			// In production, this should be logged for manual cleanup
			fmt.Printf("Warning: failed to delete old blob %s: %v\n", m.Filename(), err)
		}
	}

	return &api.CompactionStats{
		Inputs:  cc.Inputs,
		Outputs: []*api.BlobMeta{meta},
	}
}

// mergeWithoutGC performs simple merge without garbage collection
func (c *Compactor) mergeWithoutGC(ctx context.Context, mr *sstable.MergeReader, ch chan<- *types.Record) error {
	for {
		rec, err := mr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("MergeReader.Next: %w", err)
		}
		
		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// mergeWithGC performs merge with garbage collection policy applied
func (c *Compactor) mergeWithGC(ctx context.Context, mr *sstable.MergeReader, ch chan<- *types.Record, gcPolicy *api.GCPolicy) error {
	now := c.clock.Now()
	versionCount := make(map[string]int)
	
	for {
		rec, err := mr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("MergeReader.Next: %w", err)
		}
		
		// Apply GC policy to decide whether to keep this record
		if c.shouldKeepRecord(rec, gcPolicy, now, versionCount) {
			select {
			case ch <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// shouldKeepRecord determines if a record should be kept based on GC policy
func (c *Compactor) shouldKeepRecord(rec *types.Record, policy *api.GCPolicy, now time.Time, versionCount map[string]int) bool {
	// Check age limits
	if policy.MaxAge > 0 && now.Sub(rec.Timestamp) > policy.MaxAge {
		return false
	}
	
	// Check tombstone age limits
	if rec.Tombstone && policy.TombstoneGCAge > 0 && now.Sub(rec.Timestamp) > policy.TombstoneGCAge {
		return false
	}
	
	// Check version limits
	if policy.MaxVersions > 0 {
		versionCount[rec.Key]++
		if versionCount[rec.Key] > policy.MaxVersions {
			return false
		}
	}
	
	return true
}
