package iterator

import (
	"container/heap"
	"context"
	"time"

	"slices"

	"github.com/adammck/blobby/pkg/api"
)

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

func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

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

		if advanceIterState(ctx, s) {
			heap.Push(c.heap, s)
		}
	}

	return c
}

func advanceIterState(ctx context.Context, s *iterState) bool {
	if !s.iter.Next(ctx) {
		s.valid = false
		return false
	}

	s.key = s.iter.Key()
	s.value = s.iter.Value()
	s.timestamp = s.iter.Timestamp()
	s.valid = true

	return true
}

func (ci *Compound) Next(ctx context.Context) bool {
	if ci.exhausted || ci.err != nil || ci.closed {
		return false
	}

	select {
	case <-ctx.Done():
		ci.err = ctx.Err()
		return false
	default:
	}

	for {
		if ci.heap.Len() == 0 {
			ci.exhausted = true
			return false
		}

		s := heap.Pop(ci.heap).(*iterState)

		if s.key == ci.lastKey {
			if advanceIterState(ctx, s) {
				heap.Push(ci.heap, s)
			}
			continue
		}

		if len(s.value) == 0 {
			ci.lastKey = s.key
			if advanceIterState(ctx, s) {
				heap.Push(ci.heap, s)
			}
			continue
		}

		ci.current = &iterState{
			key:   s.key,
			value: slices.Clone(s.value),
		}
		ci.lastKey = s.key

		if advanceIterState(ctx, s) {
			heap.Push(ci.heap, s)
		}

		return true
	}
}

func (ci *Compound) Key() string {
	if ci.current == nil || ci.closed {
		return ""
	}

	return ci.current.key
}

func (ci *Compound) Value() []byte {
	if ci.current == nil || ci.closed {
		return nil
	}

	return ci.current.value
}

func (ci *Compound) Timestamp() time.Time {
	if ci.current == nil || ci.closed {
		return time.Time{}
	}

	return ci.current.timestamp
}

func (ci *Compound) Err() error {
	return ci.err
}

func (ci *Compound) Close() error {
	if ci.closed {
		return nil
	}
	ci.closed = true

	for _, iter := range ci.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}

	return nil
}
