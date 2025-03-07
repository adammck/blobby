package sstable

import (
	"fmt"
	"io"

	"github.com/adammck/blobby/pkg/types"
)

type Reader struct {
	// TODO: this should be ReadCloser. i think we're leaking connections.
	r io.Reader
}

func NewReader(r io.Reader) (*Reader, error) {
	magic := make([]byte, len(magicBytes))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic bytes: %w", err)
	}
	if string(magic) != magicBytes {
		return nil, fmt.Errorf("wrong magic bytes: got=%x, want=%x", magic, magicBytes)
	}

	return &Reader{
		r: r,
	}, nil
}

func (r *Reader) Next() (*types.Record, error) {
	return types.Read(r.r)
}
