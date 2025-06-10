package memtable

import (
	"context"
	"time"

	"github.com/adammck/blobby/pkg/types"
)

type cursor interface {
	Next(ctx context.Context) bool
	Err() error
	Decode(v interface{}) error
	Close(ctx context.Context) error
}

// memtableIterator implements api.Iterator for a single memtable
type memtableIterator struct {
	cursor cursor
	ctx    context.Context
	err    error
	cur    *types.Record
}

func (it *memtableIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}

	if !it.cursor.Next(ctx) {
		it.err = it.cursor.Err()
		return false
	}

	var rec types.Record
	if err := it.cursor.Decode(&rec); err != nil {
		it.err = err
		return false
	}

	it.cur = &rec
	return true
}

func (it *memtableIterator) Key() string {
	if it.cur == nil {
		return ""
	}
	return it.cur.Key
}

func (it *memtableIterator) Value() []byte {
	if it.cur == nil {
		return nil
	}
	return it.cur.Document
}

func (it *memtableIterator) Err() error {
	return it.err
}

func (it *memtableIterator) Close() error {
	return it.cursor.Close(it.ctx)
}

// Timestamp returns the timestamp of the current record
func (it *memtableIterator) Timestamp() time.Time {
	if it.cur == nil {
		return time.Time{}
	}
	return it.cur.Timestamp
}
