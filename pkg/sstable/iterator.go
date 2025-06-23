package sstable

import (
	"context"
	"io"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/types"
)

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

// Timestamp returns the timestamp of the current record
func (it *rangeIterator) Timestamp() time.Time {
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
