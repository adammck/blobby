package sstable

import (
	"fmt"
	"io"

	"github.com/adammck/archive/pkg/types"
)

type Reader struct {
	r io.Reader
}

func NewReader(r io.Reader) (*Reader, error) {
	magic := make([]byte, len(magicBytes))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic bytes: %w", err)
	}
	if string(magic) != magicBytes {
		return nil, fmt.Errorf("wrong magic bytes")
	}

	return &Reader{
		r: r,
	}, nil
}

func (r *Reader) Next() (*types.Record, error) {
	return types.Read(r.r)
}
