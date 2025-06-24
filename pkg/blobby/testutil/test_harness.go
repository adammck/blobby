package testutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/stretchr/testify/require"
)

type Harness struct {
	sut   api.Blobby
	model *FakeBlobby
	keys  map[string]struct{} // track keys we've inserted
	stats Stats
}

func NewHarness(sut api.Blobby) *Harness {
	return &Harness{
		sut:   sut,
		model: NewFakeBlobby(),
		keys:  make(map[string]struct{}),
	}
}

type Op interface {
	Run(t *testing.T, ctx context.Context) error
	String() string
}

func (s *Harness) Get(key string) Op {
	return GetOp{h: s, k: key}
}

func (s *Harness) Put(key string, value []byte) Op {
	return PutOp{h: s, k: key, v: value}
}

func (s *Harness) Delete(key string) Op {
	return DeleteOp{h: s, k: key}
}

func (s *Harness) Flush() Op {
	return FlushOp{h: s}
}

func (s *Harness) Compact() Op {
	return CompactOp{h: s}
}

func (s *Harness) Scan(start, end string) Op {
	return ScanOp{h: s, start: start, end: end}
}

func (h *Harness) Verify(ctx context.Context, t *testing.T) {
	t.Log("Verifying final state...")
	var verified int

	for key := range h.keys {
		expected, _, expectedErr := h.model.Get(ctx, key)
		actual, stats, actualErr := h.sut.Get(ctx, key)

		// model errors other than notfound are test failures
		if expectedErr != nil && !errors.Is(expectedErr, &api.NotFound{}) {
			require.Fail(t, "model error", "key %s: %v", key, expectedErr)
		}

		// if model says notfound, sut should too
		if errors.Is(expectedErr, &api.NotFound{}) {
			require.ErrorIs(t, actualErr, &api.NotFound{}, "key %s: model notfound but sut found", key)
			verified++
			continue
		}

		// model found value, sut should too
		require.NoError(t, actualErr, "key %s", key)
		require.Equal(t, expected, actual,
			"key %s: expected %q, got %q from %s",
			key, expected, actual, stats.Source)

		if actualErr == nil {
			h.stats.Incr(stats)
		}
		verified++
	}

	t.Logf("Verified %d keys", verified)
}

func (h *Harness) LogStats(t *testing.T) {
	t.Log("Stats:")
	t.Logf("- blobs fetched: %d", h.stats.BlobsFetched)
	t.Logf("- blobs skipped: %d", h.stats.BlobsSkipped)
	t.Logf("- records scanned: %d", h.stats.TotalRecordsScanned)
	t.Logf("- worst scan: %d recs", h.stats.MaxRecordsScanned)
	t.Logf("- mean scan: %.1f recs", h.stats.MeanRecordsScanned())
}

type Stats struct {
	NumGets             uint64
	BlobsFetched        uint64
	BlobsSkipped        uint64
	TotalRecordsScanned uint64
	MaxRecordsScanned   uint64
}

func (s *Stats) Incr(stats *api.GetStats) {
	s.NumGets += 1
	s.BlobsFetched += uint64(stats.BlobsFetched)
	s.BlobsSkipped += uint64(stats.BlobsSkipped)
	s.TotalRecordsScanned += uint64(stats.RecordsScanned)
	if uint64(stats.RecordsScanned) > s.MaxRecordsScanned {
		s.MaxRecordsScanned = uint64(stats.RecordsScanned)
	}
}

// MeanRecordsScanned returns the mean number of records scanned per get
func (s *Stats) MeanRecordsScanned() float64 {
	return float64(s.TotalRecordsScanned) / float64(s.NumGets)
}

type PutOp struct {
	k string
	v []byte
	h *Harness
}

func (o PutOp) String() string {
	return fmt.Sprintf("put %s=%q", o.k, o.v)
}

func (o PutOp) Run(t *testing.T, ctx context.Context) error {
	dest, err := o.h.sut.Put(ctx, o.k, o.v)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	_, err = o.h.model.Put(ctx, o.k, o.v)
	if err != nil {
		return fmt.Errorf("fakeBlobby put: %v", err)
	}

	o.h.keys[o.k] = struct{}{}
	t.Logf("Put %s=%q -> %s", o.k, o.v, dest.Destination)

	return nil
}

type GetOp struct {
	h *Harness
	k string
}

func (o GetOp) String() string {
	return fmt.Sprintf("get %s", o.k)
}

func (o GetOp) Run(t *testing.T, ctx context.Context) error {
	val, stats, err := o.h.sut.Get(ctx, o.k)

	// handle unexpected errors early
	if err != nil && !errors.Is(err, &api.NotFound{}) {
		return fmt.Errorf("get: %v", err)
	}

	// handle notfound case early
	if errors.Is(err, &api.NotFound{}) {
		_, _, modelErr := o.h.model.Get(ctx, o.k)
		if !errors.Is(modelErr, &api.NotFound{}) {
			return fmt.Errorf("sut returned NotFound but model did not: sut=%v, model=%v", err, modelErr)
		}
		t.Logf("Get %s -> NotFound (expected)", o.k)
		return nil
	}

	// success case - compare with model
	modelVal, _, modelErr := o.h.model.Get(ctx, o.k)
	if modelErr != nil {
		return fmt.Errorf("model get: %v", modelErr)
	}

	if !bytes.Equal(val, modelVal) {
		return fmt.Errorf("value mismatch: sut=%q, model=%q", val, modelVal)
	}

	o.h.stats.Incr(stats)
	t.Logf("Get %s=%q from %s", o.k, val, stats.Source)

	return nil
}

type DeleteOp struct {
	h *Harness
	k string
}

func (o DeleteOp) String() string {
	return fmt.Sprintf("delete %s", o.k)
}

func (o DeleteOp) Run(t *testing.T, ctx context.Context) error {
	stats, err := o.h.sut.Delete(ctx, o.k)
	if err != nil {
		return fmt.Errorf("sut.Delete: %v", err)
	}

	_, err = o.h.model.Delete(ctx, o.k)
	if err != nil {
		return fmt.Errorf("model.Delete: %v", err)
	}

	t.Logf("Delete %s -> %s", o.k, stats.Destination)

	return nil
}

type FlushOp struct {
	h *Harness
}

func (o FlushOp) String() string {
	return "flush"
}

// Run executes the flush operation
func (o FlushOp) Run(t *testing.T, ctx context.Context) error {
	stats, err := o.h.sut.Flush(ctx)
	if err != nil {
		// special case. it's fine if there's nothing to flush.
		if errors.Is(err, sstable.ErrNoRecords) {
			t.Logf("Flush: no records.")
			return nil
		}

		return fmt.Errorf("flush: %v", err)
	}

	t.Logf("Flush: %d records -> %s, now active: %s",
		stats.Meta.Count, stats.Meta.Filename(), stats.ActiveMemtable)

	return nil
}

type CompactOp struct {
	h *Harness
}

func (o CompactOp) String() string {
	return "compact"
}

// Run executes the compact operation
func (o CompactOp) Run(t *testing.T, ctx context.Context) error {
	stats, err := o.h.sut.Compact(ctx, api.CompactionOptions{})
	if err != nil {
		return fmt.Errorf("compact: %v", err)
	}

	// TODO: print all of the stats, not just the first.
	if len(stats) > 0 {
		t.Logf("Compact: %d input files, %d output files",
			len(stats[0].Inputs), len(stats[0].Outputs))
	}

	return nil
}

type ScanOp struct {
	h     *Harness
	start string
	end   string
}

func (o ScanOp) String() string {
	return fmt.Sprintf("scan [%q, %q)", o.start, o.end)
}

func (o ScanOp) Run(t *testing.T, ctx context.Context) error {
	iter, _, err := o.h.sut.Scan(ctx, o.start, o.end)
	if err != nil {
		return fmt.Errorf("sut.Scan: %w", err)
	}
	defer iter.Close()

	modelIter, _, err := o.h.model.Scan(ctx, o.start, o.end)
	if err != nil {
		return fmt.Errorf("model.Scan: %w", err)
	}
	defer modelIter.Close()

	// verify both iterators produce same results in same order
	var count int
	for {
		sutHasNext := iter.Next(ctx)
		modelHasNext := modelIter.Next(ctx)

		if sutHasNext != modelHasNext {
			return fmt.Errorf("iterator length mismatch at position %d", count)
		}

		if !sutHasNext {
			break
		}

		if iter.Key() != modelIter.Key() {
			return fmt.Errorf("key mismatch at position %d: sut=%q model=%q",
				count, iter.Key(), modelIter.Key())
		}

		sutVal := iter.Value()
		modelVal := modelIter.Value()
		if !bytes.Equal(sutVal, modelVal) {
			return fmt.Errorf("value mismatch for key %q: sut=%q model=%q",
				iter.Key(), sutVal, modelVal)
		}

		count++
	}

	if iter.Err() != nil {
		return fmt.Errorf("sut iterator error: %w", iter.Err())
	}

	if modelIter.Err() != nil {
		return fmt.Errorf("model iterator error: %w", modelIter.Err())
	}

	t.Logf("Scan [%q, %q) -> %d keys", o.start, o.end, count)

	return nil
}
