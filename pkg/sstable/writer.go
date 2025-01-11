package sstable

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/adammck/archive/pkg/types"
)

type Writer struct {
	records []*types.Record
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Add(record *types.Record) error {
	w.records = append(w.records, record)
	return nil
}

func (w *Writer) Write(out io.Writer) (*Meta, error) {
	sort.Slice(w.records, func(i, j int) bool {
		return w.records[i].Key < w.records[j].Key
	})

	_, err := out.Write([]byte(magicBytes))
	if err != nil {
		return nil, err
	}

	m := &Meta{
		Created: time.Now(),
		Size:    int64(len(magicBytes)),
	}

	for _, record := range w.records {
		n, err := record.Write(out)
		if err != nil {
			return nil, fmt.Errorf("write record: %w", err)
		}

		m.Count++
		m.Size += int64(n)

		if m.MinKey == "" || record.Key < m.MinKey {
			m.MinKey = record.Key
		}

		if m.MaxKey == "" || record.Key > m.MaxKey {
			m.MaxKey = record.Key
		}
	}

	return m, nil
}
