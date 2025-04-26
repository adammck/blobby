package sstable

import (
	"fmt"
	"io"

	"github.com/adammck/blobby/pkg/types"
)

type Reader struct {
	rc io.ReadCloser
}

// NewReader is for reading an entire SSTable, including the header. It
// validates the header, then returns a reader to consume the records.
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

// NewPartialReader is like NewReader, but doesn't validate the header. This is
// useful when reading a fragment of an sstable, when the relevant byte range
// has been determined via an index.
func NewPartialReader(rc io.ReadCloser) *Reader {
	return &Reader{
		rc: rc,
	}
}

func (r *Reader) Next() (*types.Record, error) {
	rec, err := types.Read(r.rc)
	if rec == nil && err == nil {
		return nil, io.EOF
	}
	return rec, err
}

func (r *Reader) Close() error {
	return r.rc.Close()
}
