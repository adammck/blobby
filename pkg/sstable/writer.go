package sstable

import (
	"fmt"
	"io"
	"time"

	"github.com/adammck/archive/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
)

type Writer struct {
	w    io.Writer
	meta *Meta
}

func NewWriter(w io.Writer) (*Writer, error) {
	_, err := w.Write([]byte(magicBytes))
	if err != nil {
		return nil, err
	}

	return &Writer{
		w: w,
		meta: &Meta{
			Created: time.Now(),
			Size:    int64(len(magicBytes)),
		},
	}, nil
}

func (w *Writer) Write(record *types.Record) error {
	b, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}

	_, err = w.w.Write(b)
	if err != nil {
		return fmt.Errorf("Write: %w", err)
	}

	w.meta.Count++
	w.meta.Size += int64(len(b))

	if w.meta.MinKey == "" || record.Key < w.meta.MinKey {
		w.meta.MinKey = record.Key
	}

	if w.meta.MaxKey == "" || record.Key > w.meta.MaxKey {
		w.meta.MaxKey = record.Key
	}

	return nil
}

func (w *Writer) Meta() *Meta {
	return w.meta
}
