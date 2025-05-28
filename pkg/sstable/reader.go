package sstable

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/adammck/blobby/pkg/api"
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
	return types.Read(r.rc)
}

func (r *Reader) Close() error {
	return r.rc.Close()
}

// rangeIterator wraps an sstable Reader to provide range scanning
type rangeIterator struct {
	reader    *Reader
	start     string
	end       string
	current   *types.Record
	err       error
	exhausted bool
}

func (it *rangeIterator) Next(ctx context.Context) bool {
	if it.exhausted || it.err != nil {
		return false
	}

	for {
		rec, err := it.reader.Next()
		if err != nil {
			if err == io.EOF {
				it.exhausted = true
				return false
			}
			it.err = err
			return false
		}
		if rec == nil {
			it.exhausted = true
			return false
		}

		// check if we're past the end of range
		if it.end != "" && rec.Key >= it.end {
			it.exhausted = true
			return false
		}

		// check if we're before the start of range
		if it.start != "" && rec.Key < it.start {
			continue
		}

		it.current = rec
		return true
	}
}

func (it *rangeIterator) Key() string {
	if it.current == nil {
		return ""
	}
	return it.current.Key
}

func (it *rangeIterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.Document
}

// CurrentTimestamp returns the timestamp of the current record
func (it *rangeIterator) CurrentTimestamp() time.Time {
	if it.current == nil {
		return time.Time{}
	}
	return it.current.Timestamp
}

func (it *rangeIterator) Err() error {
	return it.err
}

func (it *rangeIterator) Close() error {
	return it.reader.Close()
}

// NewRangeIterator creates an iterator that scans within a key range
func NewRangeIterator(reader *Reader, start, end string) api.Iterator {
	return &rangeIterator{
		reader: reader,
		start:  start,
		end:    end,
	}
}
