package memtable

import (
	"context"
	"errors"
	"time"

	"github.com/adammck/blobby/pkg/types"
)

type simpleMockCursor struct {
	records []types.Record
	pos     int
	err     error
	closed  bool
}

func newMockCursor(records []types.Record) *simpleMockCursor {
	return &simpleMockCursor{
		records: records,
		pos:     -1,
	}
}

func (c *simpleMockCursor) Next(ctx context.Context) bool {
	if c.err != nil {
		return false
	}
	if c.closed {
		return false
	}
	c.pos++
	return c.pos < len(c.records)
}

func (c *simpleMockCursor) Err() error {
	return c.err
}

func (c *simpleMockCursor) Decode(v interface{}) error {
	if c.err != nil {
		return c.err
	}
	if c.pos < 0 || c.pos >= len(c.records) {
		return errors.New("no current record")
	}

	rec, ok := v.(*types.Record)
	if !ok {
		return errors.New("decode target must be *types.Record")
	}

	src := c.records[c.pos]
	*rec = src

	// simulate real mongo behavior: tombstone records don't have document data
	if rec.Tombstone {
		rec.Document = nil
	}

	// ensure timestamp is set if not already (mock records might not have it)
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().Add(time.Duration(c.pos) * time.Millisecond)
	}

	return nil
}

func (c *simpleMockCursor) Close(ctx context.Context) error {
	c.closed = true
	return nil
}

type failingMockCursor struct {
	decodeErr error
}

func newFailingMockCursor(decodeErr error) *failingMockCursor {
	return &failingMockCursor{
		decodeErr: decodeErr,
	}
}

func (c *failingMockCursor) Next(ctx context.Context) bool {
	return true
}

func (c *failingMockCursor) Err() error {
	return nil
}

func (c *failingMockCursor) Decode(v interface{}) error {
	return c.decodeErr
}

func (c *failingMockCursor) Close(ctx context.Context) error {
	return nil
}
