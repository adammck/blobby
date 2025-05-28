package blobby

import (
	"container/heap"
	"context"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/types"
)

// recordIterator is an internal interface for iterating over full Record objects
type recordIterator interface {
	Next(ctx context.Context) (*types.Record, error)
	Close() error
}

// timestampProvider is an interface for iterators that can provide the current record's timestamp
type timestampProvider interface {
	CurrentTimestamp() time.Time
}

// apiIteratorAdapter adapts api.Iterator to recordIterator by reconstructing Record objects
type apiIteratorAdapter struct {
	iter api.Iterator
}

func (a *apiIteratorAdapter) Next(ctx context.Context) (*types.Record, error) {
	if !a.iter.Next(ctx) {
		if err := a.iter.Err(); err != nil {
			return nil, err
		}
		return nil, nil // EOF
	}

	// extract real timestamp from underlying iterator if possible
	var timestamp time.Time
	if provider, ok := a.iter.(timestampProvider); ok {
		timestamp = provider.CurrentTimestamp()
	} else {
		// fallback to current time if we can't extract the real timestamp
		timestamp = time.Now()
	}

	return &types.Record{
		Key:       a.iter.Key(),
		Document:  a.iter.Value(),
		Timestamp: timestamp,
		Tombstone: len(a.iter.Value()) == 0, // simple heuristic
	}, nil
}

func (a *apiIteratorAdapter) Close() error {
	return a.iter.Close()
}

// compoundIterator merges multiple recordIterators while handling duplicates and tombstones
type compoundIterator struct {
	ctx            context.Context
	heap           *iteratorHeap
	current        *types.Record
	lastEmittedKey string
	err            error
	exhausted      bool
}

type iteratorState struct {
	iter    recordIterator
	current *types.Record
	source  string // for debugging
}

type iteratorHeap []*iteratorState

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	// first by key (ascending)
	if h[i].current.Key != h[j].current.Key {
		return h[i].current.Key < h[j].current.Key
	}
	// then by timestamp (descending) for same key - newer first
	return h[i].current.Timestamp.After(h[j].current.Timestamp)
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

func newCompoundIterator(ctx context.Context, iterators []recordIterator, sources []string) *compoundIterator {
	ci := &compoundIterator{
		ctx:  ctx,
		heap: &iteratorHeap{},
	}

	// prime all iterators and add to heap
	for i, iter := range iterators {
		rec, err := iter.Next(ctx)
		if err != nil {
			ci.err = err
			return ci
		}
		if rec != nil {
			state := &iteratorState{
				iter:    iter,
				current: rec,
				source:  sources[i],
			}
			heap.Push(ci.heap, state)
		}
	}

	heap.Init(ci.heap)
	return ci
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
		rec := state.current

		// advance that iterator
		next, err := state.iter.Next(ctx)
		if err != nil {
			ci.err = err
			return false
		}
		if next != nil {
			state.current = next
			heap.Push(ci.heap, state)
		}

		// skip if we've seen this key (older version)
		if rec.Key == ci.lastEmittedKey {
			continue
		}

		// skip if tombstone
		if rec.Tombstone {
			ci.lastEmittedKey = rec.Key
			continue
		}

		// found a live record
		ci.current = rec
		ci.lastEmittedKey = rec.Key
		return true
	}
}

func (ci *compoundIterator) Key() string {
	if ci.current == nil {
		return ""
	}
	return ci.current.Key
}

func (ci *compoundIterator) Value() []byte {
	if ci.current == nil {
		return nil
	}
	return ci.current.Document
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
