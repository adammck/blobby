package sstable

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/adammck/archive/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
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

		// todo: move marshalling/writing to Record
		b, err := bson.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("encode record: %w", err)
		}

		_, err = out.Write(b)
		if err != nil {
			return nil, fmt.Errorf("Write: %w", err)
		}

		m.Count++
		m.Size += int64(len(b))

		if m.MinKey == "" || record.Key < m.MinKey {
			m.MinKey = record.Key
		}

		if m.MaxKey == "" || record.Key > m.MaxKey {
			m.MaxKey = record.Key
		}
	}

	return m, nil
}
