package blobby

import (
	"context"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

// countingIterator wraps another iterator and tracks the number of records returned
type countingIterator struct {
	inner api.Iterator
	stats *api.ScanStats
}

func newCountingIterator(inner api.Iterator, stats *api.ScanStats) *countingIterator {
	return &countingIterator{
		inner: inner,
		stats: stats,
	}
}

func (c *countingIterator) Next(ctx context.Context) bool {
	if c.inner == nil {
		return false
	}
	hasNext := c.inner.Next(ctx)
	if hasNext {
		c.stats.RecordsReturned++
	}
	return hasNext
}

func (c *countingIterator) Key() string {
	if c.inner == nil {
		return ""
	}
	return c.inner.Key()
}

func (c *countingIterator) Value() []byte {
	if c.inner == nil {
		return nil
	}
	return c.inner.Value()
}

func (c *countingIterator) Timestamp() time.Time {
	if c.inner == nil {
		return time.Time{}
	}
	return c.inner.Timestamp()
}

func (c *countingIterator) Err() error {
	if c.inner == nil {
		return nil
	}
	return c.inner.Err()
}

func (c *countingIterator) Close() error {
	if c.inner == nil {
		return nil
	}
	return c.inner.Close()
}