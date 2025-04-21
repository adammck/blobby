package sstable

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
)

const (
	defaultFilterType = "xor"
)

type Writer struct {
	records []*types.Record
	mu      sync.Mutex
	clock   clockwork.Clock

	// set via options on constructor.

	indexRecordFreq int
	indexByteFreq   int
	filterType      string
}

type WriterOption func(*Writer)

// WithIndexEveryNRecords instructs the writer to emit an index entry every n
// records, starting with the first.
func WithIndexEveryNRecords(n int) WriterOption {
	return func(w *Writer) {
		w.indexRecordFreq = n
	}
}

// WithIndexEveryNBytes instructs the writer to emit an index entry every n
// bytes. This is not exact, because the size of an entry is unknown until it's
// wrritten, but it'll exceed n by one record at most.
func WithIndexEveryNBytes(n int) WriterOption {
	return func(w *Writer) {
		w.indexByteFreq = n
	}
}

// WithFilter sets the filter type to use. See defaultFilterType for the
// default value.
func WithFilter(ft string) WriterOption {
	return func(w *Writer) {
		w.filterType = ft
	}
}

// TODO: Make the clock an option, too.
func NewWriter(clock clockwork.Clock, options ...WriterOption) *Writer {
	w := &Writer{
		clock:      clock,
		filterType: defaultFilterType,
	}

	for _, opt := range options {
		opt(w)
	}

	return w
}

func (w *Writer) Add(record *types.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, record)
	return nil
}

// Write writes the SSTable to the given writer, and returns the corresponding
// index entries which should be persited somewhere via an IndexStore.
func (w *Writer) Write(out io.Writer) (*api.BlobMeta, []api.IndexEntry, api.FilterDecoded, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// sort first by key, in ascending order. but within a key, sort timestamps
	// in *descending* order, so the newest one (i.e. the highest timestamp) is
	// first. this way, when scanning for the newest, we can as soon as we find
	// a single key.
	slices.SortFunc(w.records, func(a, b *types.Record) int {
		c := strings.Compare(a.Key, b.Key)
		if c != 0 {
			return c
		}

		return b.Timestamp.Compare(a.Timestamp)
	})

	n, err := out.Write([]byte(magicBytes))
	if err != nil {
		return nil, nil, nil, err
	}

	m := &api.BlobMeta{
		Created: w.clock.Now(),
		Size:    int64(n),
	}

	offset := int64(n)
	idxRecs := 0
	idxBytes := 0

	var idx []api.IndexEntry

	prevKey := ""
	keys := []string{}

	for i, record := range w.records {
		if i == 0 || w.shouldCreateIndex(idxRecs, idxBytes) {
			idx = append(idx, api.IndexEntry{
				Key:    record.Key,
				Offset: offset,
			})
			idxRecs = 0
			idxBytes = 0
		}

		n, err := record.Write(out)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("record.Write: %w", err)
		}

		offset += int64(n)
		idxBytes += n
		idxRecs++

		// Update metadata
		m.Count++
		m.Size += int64(n)

		if m.MinKey == "" || record.Key < m.MinKey {
			m.MinKey = record.Key
		}

		if m.MaxKey == "" || record.Key > m.MaxKey {
			m.MaxKey = record.Key
		}

		if m.MinTime.IsZero() || record.Timestamp.Before(m.MinTime) {
			m.MinTime = record.Timestamp
		}

		if m.MaxTime.IsZero() || record.Timestamp.After(m.MaxTime) {
			m.MaxTime = record.Timestamp
		}

		// accumulate keys for the (bloom) filter, ignoring timestamps.
		if prevKey != record.Key {
			keys = append(keys, record.Key)
			prevKey = record.Key
		}
	}

	// construct the filter in a single call, since some types (e.g. xor) can't
	// be built incrementally. it would be nice to encapsulat the accumulation,
	// but this seems fine for now.
	f, err := filter.Create(w.filterType, keys)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("filter.Create: %w", err)
	}

	return m, idx, f, nil
}

func (w *Writer) shouldCreateIndex(recordsSinceIndex, bytesSinceIndex int) bool {
	if w.indexRecordFreq > 0 {
		return recordsSinceIndex >= w.indexRecordFreq
	}

	if w.indexByteFreq > 0 {
		return bytesSinceIndex >= w.indexByteFreq
	}

	return false
}
