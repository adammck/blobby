package iterator

import (
	"container/heap"
	"context"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

type Compound struct {
	ctx            context.Context
	heap           *iteratorHeap
	current        *iteratorState
	lastEmittedKey string
	err            error
	exhausted      bool
	allIterators   []api.Iterator
	closed         bool
}

type iteratorState struct {
	iter      api.Iterator
	key       string
	value     []byte
	timestamp time.Time
	source    string
	valid     bool
}

type iteratorHeap []*iteratorState

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].timestamp.After(h[j].timestamp)
}

func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*iteratorState))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func NewCompound(ctx context.Context, iterators []api.Iterator, sources []string) *Compound {
	ci := &Compound{
		ctx:          ctx,
		heap:         &iteratorHeap{},
		allIterators: append([]api.Iterator(nil), iterators...),
	}

	for i, iter := range iterators {
		state := &iteratorState{
			iter:   iter,
			source: sources[i],
		}

		if advanceIteratorState(ctx, state) {
			heap.Push(ci.heap, state)
		}
	}

	return ci
}

func advanceIteratorState(ctx context.Context, state *iteratorState) bool {
	if !state.iter.Next(ctx) {
		state.valid = false
		return false
	}

	state.key = state.iter.Key()
	state.value = state.iter.Value()
	state.timestamp = state.iter.Timestamp()
	state.valid = true

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

		state := heap.Pop(ci.heap).(*iteratorState)

		if state.key == ci.lastEmittedKey {
			if advanceIteratorState(ctx, state) {
				heap.Push(ci.heap, state)
			}
			continue
		}

		if len(state.value) == 0 {
			ci.lastEmittedKey = state.key
			if advanceIteratorState(ctx, state) {
				heap.Push(ci.heap, state)
			}
			continue
		}

		ci.current = &iteratorState{
			key:   state.key,
			value: append([]byte(nil), state.value...),
		}
		ci.lastEmittedKey = state.key

		if advanceIteratorState(ctx, state) {
			heap.Push(ci.heap, state)
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

	for _, iter := range ci.allIterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}
