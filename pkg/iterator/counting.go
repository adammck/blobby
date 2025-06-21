package iterator

import (
	"context"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

// Counting wraps another iterator and tracks the number of records returned.
// It updates the RecordsReturned field in the provided ScanStats for each
// record consumed via Next().
type Counting struct {
	inner api.Iterator
	stats *api.ScanStats
}

// NewCounting creates a new counting iterator that wraps the given iterator
// and increments the RecordsReturned field in stats for each record consumed.
func NewCounting(inner api.Iterator, stats *api.ScanStats) *Counting {
	return &Counting{
		inner: inner,
		stats: stats,
	}
}

func (c *Counting) Next(ctx context.Context) bool {
	if c.inner == nil {
		return false
	}
	hasNext := c.inner.Next(ctx)
	if hasNext {
		c.stats.RecordsReturned++
	}
	return hasNext
}

func (c *Counting) Key() string {
	if c.inner == nil {
		return ""
	}
	return c.inner.Key()
}

func (c *Counting) Value() []byte {
	if c.inner == nil {
		return nil
	}
	return c.inner.Value()
}

func (c *Counting) Timestamp() time.Time {
	if c.inner == nil {
		return time.Time{}
	}
	return c.inner.Timestamp()
}

func (c *Counting) Err() error {
	if c.inner == nil {
		return nil
	}
	return c.inner.Err()
}

func (c *Counting) Close() error {
	if c.inner == nil {
		return nil
	}
	return c.inner.Close()
}