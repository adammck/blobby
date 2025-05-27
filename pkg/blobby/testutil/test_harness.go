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

func (h *Harness) Verify(ctx context.Context, t *testing.T) {
	t.Log("Verifying final state...")
	var verified int

	for key := range h.keys {
		expected, _, err := h.model.Get(ctx, key)
		require.NoError(t, err)

		actual, stats, err := h.sut.Get(ctx, key)
		require.NoError(t, err)

		require.Equal(t, expected, actual,
			"key %s: expected %q, got %q from %s",
			key, expected, actual, stats.Source)

		h.stats.Incr(stats)
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
	t.Logf("Put %s=%q -> %s", o.k, o.v, dest)

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
	if err != nil {
		// check if it's a NotFound error - that's expected for deleted/missing keys
		if errors.Is(err, &api.NotFound{}) {
			// verify model also returns not found
			_, _, modelErr := o.h.model.Get(ctx, o.k)
			if !errors.Is(modelErr, &api.NotFound{}) {
				return fmt.Errorf("sut returned NotFound but model did not: sut=%v, model=%v", err, modelErr)
			}
			t.Logf("Get %s -> NotFound (expected)", o.k)
			return nil
		}
		return fmt.Errorf("get: %v", err)
	}

	// compare with model
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
		return fmt.Errorf("delete: %v", err)
	}

	_, err = o.h.model.Delete(ctx, o.k)
	if err != nil {
		return fmt.Errorf("model delete: %v", err)
	}

	t.Logf("Delete %s -> %v", o.k, stats.Timestamp)

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
