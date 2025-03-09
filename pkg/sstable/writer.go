package sstable

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
)

type Writer struct {
	records []*types.Record
	mu      sync.Mutex
	clock   clockwork.Clock

	indexRecordFreq int
	indexByteFreq   int
}

type WriterOption func(*Writer)

func WithIndexEveryNRecords(n int) WriterOption {
	return func(w *Writer) {
		w.indexRecordFreq = n
	}
}

func WithIndexEveryNBytes(n int) WriterOption {
	return func(w *Writer) {
		w.indexByteFreq = n
	}
}

// TODO: Make the clock an option, too.
func NewWriter(clock clockwork.Clock, options ...WriterOption) *Writer {
	w := &Writer{
		clock: clock,
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

// Enhanced Write method that builds and stores indices
func (w *Writer) Write(out io.Writer) (*Meta, api.Index, error) {
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
		return nil, nil, err
	}

	m := &Meta{
		Created: w.clock.Now(),
		Size:    int64(n),
	}

	offset := int64(n)
	idxRecs := 0
	idxBytes := 0

	var idx api.Index
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
			return nil, nil, fmt.Errorf("record.Write: %w", err)
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
	}

	return m, idx, nil
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
