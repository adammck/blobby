package blobby

import (
	"container/heap"
	"context"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

// timestampProvider extracts timestamp from iterator
type timestampProvider interface {
	CurrentTimestamp() time.Time
}

// compoundIterator merges multiple api.Iterators while handling duplicates and tombstones
type compoundIterator struct {
	ctx            context.Context
	heap           *iteratorHeap
	current        *iteratorState
	lastEmittedKey string
	err            error
	exhausted      bool
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
	// first by key (ascending)
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	// then by timestamp (descending) for same key - newer first
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

func newCompoundIterator(ctx context.Context, iterators []api.Iterator, sources []string) *compoundIterator {
	ci := &compoundIterator{
		ctx:  ctx,
		heap: &iteratorHeap{},
	}

	// prime all iterators and add to heap
	for i, iter := range iterators {
		state := &iteratorState{
			iter:   iter,
			source: sources[i],
		}

		if advanceIteratorState(ctx, state) {
			heap.Push(ci.heap, state)
		}
	}

	heap.Init(ci.heap)
	return ci
}

func advanceIteratorState(ctx context.Context, state *iteratorState) bool {
	if !state.iter.Next(ctx) {
		state.valid = false
		return false
	}

	state.key = state.iter.Key()
	state.value = state.iter.Value()
	state.valid = true

	// all iterators used in range scans implement timestampProvider
	provider := state.iter.(timestampProvider)
	state.timestamp = provider.CurrentTimestamp()

	return true
}

func (ci *compoundIterator) Next(ctx context.Context) bool {
	if ci.exhausted || ci.err != nil {
		return false
	}

	// check context cancellation
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

		// get next record from heap
		state := heap.Pop(ci.heap).(*iteratorState)

		// advance that iterator
		if advanceIteratorState(ctx, state) {
			heap.Push(ci.heap, state)
		}

		// skip if we've seen this key (older version)
		if state.key == ci.lastEmittedKey {
			continue
		}

		// skip if tombstone
		if len(state.value) == 0 {
			ci.lastEmittedKey = state.key
			continue
		}

		// found a live record
		ci.current = state
		ci.lastEmittedKey = state.key
		return true
	}
}

func (ci *compoundIterator) Key() string {
	if ci.current == nil {
		return ""
	}
	return ci.current.key
}

func (ci *compoundIterator) Value() []byte {
	if ci.current == nil {
		return nil
	}
	return ci.current.value
}

func (ci *compoundIterator) Err() error {
	return ci.err
}

func (ci *compoundIterator) Close() error {
	// close all underlying iterators
	for ci.heap.Len() > 0 {
		state := heap.Pop(ci.heap).(*iteratorState)
		if err := state.iter.Close(); err != nil {
			return err
		}
	}
	return nil
}
