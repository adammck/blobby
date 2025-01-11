package sstable

import (
	"fmt"
	"io"

	"github.com/adammck/archive/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
)

type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) (*Writer, error) {
	_, err := w.Write([]byte(magicBytes))
	if err != nil {
		return nil, err
	}

	return &Writer{
		w: w,
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

	return nil
}
