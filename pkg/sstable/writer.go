package sstable

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/adammck/archive/pkg/types"
	"github.com/jonboulle/clockwork"
)

type Writer struct {
	records []*types.Record
	mu      sync.Mutex
	clock   clockwork.Clock
}

func NewWriter(clock clockwork.Clock) *Writer {
	return &Writer{
		clock: clock,
	}
}

func (w *Writer) Add(record *types.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, record)
	return nil
}

func (w *Writer) Write(out io.Writer) (*Meta, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sort.Slice(w.records, func(i, j int) bool {
		// TODO: Also sort by Timestamp! We never throw records away.
		// This will be important when we start compacting sstables.
		return w.records[i].Key < w.records[j].Key
	})

	_, err := out.Write([]byte(magicBytes))
	if err != nil {
		return nil, err
	}

	m := &Meta{
		Created: w.clock.Now(),
		Size:    len(magicBytes),
	}

	for _, record := range w.records {
		n, err := record.Write(out)
		if err != nil {
			return nil, fmt.Errorf("record.Write: %w", err)
		}

		m.Count++
		m.Size += n

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

	return m, nil
}
