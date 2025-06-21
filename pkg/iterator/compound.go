// Package iterator provides utilities for merging multiple iterators
// with MVCC (Multi-Version Concurrency Control) semantics.
package iterator

import (
	"container/heap"
	"context"
	"time"

	"slices"

	"github.com/adammck/blobby/pkg/api"
)

// Compound merges multiple iterators using a min-heap to provide
// ordered iteration with MVCC semantics (most recent version wins).
type Compound struct {
	heap      *iterHeap
	current   *iterState
	lastKey   string
	err       error
	exhausted bool
	iterators []api.Iterator
	closed    bool
}

type iterState struct {
	iter      api.Iterator
	key       string
	value     []byte
	timestamp time.Time
	source    string
	valid     bool
}

type iterHeap []*iterState

func (h iterHeap) Len() int {
	return len(h)
}

func (h iterHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}

	return h[i].timestamp.After(h[j].timestamp)
}

func (h iterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iterHeap) Push(x any) {
	*h = append(*h, x.(*iterState))
}

func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// New creates a compound iterator that merges multiple iterators.
// The sources slice provides debugging information about each iterator.
func New(ctx context.Context, iters []api.Iterator, sources []string) *Compound {
	c := &Compound{
		heap:      &iterHeap{},
		iterators: slices.Clone(iters),
	}

	for i, iter := range iters {
		s := &iterState{
			iter:   iter,
			source: sources[i],
		}

		if ok, err := advanceIterState(ctx, s); err != nil {
			// Store error in compound iterator for later retrieval
			c.err = err
		} else if ok {
			heap.Push(c.heap, s)
		}
	}

	return c
}

// advanceIterState advances an iterator state to the next record.
// Returns (true, nil) if a record is available, (false, nil) if exhausted,
// or (false, error) if an error occurred.
func advanceIterState(ctx context.Context, s *iterState) (bool, error) {
	if !s.iter.Next(ctx) {
		s.valid = false
		// Check if the iterator failed with an error
		if err := s.iter.Err(); err != nil {
			return false, err
		}
		return false, nil
	}

	s.key = s.iter.Key()
	s.value = s.iter.Value()
	s.timestamp = s.iter.Timestamp()
	s.valid = true

	return true, nil
}

func (c *Compound) Next(ctx context.Context) bool {
	if c.exhausted || c.err != nil || c.closed {
		return false
	}

	select {
	case <-ctx.Done():
		c.err = ctx.Err()
		return false
	default:
	}

	for {
		if c.heap.Len() == 0 {
			c.exhausted = true
			return false
		}

		s := heap.Pop(c.heap).(*iterState)

		if s.key == c.lastKey {
			if ok, err := advanceIterState(ctx, s); err != nil {
				c.err = err
				return false
			} else if ok {
				heap.Push(c.heap, s)
			}
			continue
		}

		if len(s.value) == 0 {
			c.lastKey = s.key
			if ok, err := advanceIterState(ctx, s); err != nil {
				c.err = err
				return false
			} else if ok {
				heap.Push(c.heap, s)
			}
			continue
		}

		c.current = &iterState{
			key:       s.key,
			value:     slices.Clone(s.value),
			timestamp: s.timestamp,
		}
		c.lastKey = s.key

		if ok, err := advanceIterState(ctx, s); err != nil {
			c.err = err
			return false
		} else if ok {
			heap.Push(c.heap, s)
		}

		return true
	}
}

func (c *Compound) Key() string {
	if c.current == nil || c.closed {
		return ""
	}

	return c.current.key
}

func (c *Compound) Value() []byte {
	if c.current == nil || c.closed {
		return nil
	}

	return c.current.value
}

func (c *Compound) Timestamp() time.Time {
	if c.current == nil || c.closed {
		return time.Time{}
	}

	return c.current.timestamp
}

func (c *Compound) Err() error {
	if c.closed {
		return nil
	}
	return c.err
}

func (c *Compound) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	for _, iter := range c.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}

	return nil
}
