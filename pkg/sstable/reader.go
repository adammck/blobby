package sstable

import (
	"fmt"
	"io"

	"github.com/adammck/blobby/pkg/types"
)

type Reader struct {
	rc io.ReadCloser
}

func NewReader(rc io.ReadCloser) (*Reader, error) {
	magic := make([]byte, len(magicBytes))
	if _, err := io.ReadFull(rc, magic); err != nil {
		rc.Close()
		return nil, fmt.Errorf("read magic bytes: %w", err)
	}
	if string(magic) != magicBytes {
		rc.Close()
		return nil, fmt.Errorf("wrong magic bytes: got=%x, want=%x", magic, magicBytes)
	}

	return &Reader{
		rc: rc,
	}, nil
}

func (r *Reader) Next() (*types.Record, error) {
	return types.Read(r.rc)
}

func (r *Reader) Close() error {
	return r.rc.Close()
}
