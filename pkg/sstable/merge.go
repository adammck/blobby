// pkg/sstable/merge.go

package sstable

import (
	"container/heap"
	"fmt"
	"io"

	"github.com/adammck/archive/pkg/types"
)

type MergeReader struct {
	h recHeap
}

func NewMergeReader(readers []*Reader) (*MergeReader, error) {
	h := make(recHeap, 0, len(readers))
	heap.Init(&h)

	// init all readers with their first record.
	for i, r := range readers {
		rec, err := r.Next()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("init reader %d: %w", i, err)
		}
		if rec != nil {
			heap.Push(&h, &readerState{
				reader: r,
				rec:    rec,
			})
		}
	}

	return &MergeReader{h: h}, nil
}

func (m *MergeReader) Next() (*types.Record, error) {
	if m.h.Len() == 0 {
		return nil, io.EOF
	}

	// get the next record, in terms of key and timestamp, across all readers.
	st := heap.Pop(&m.h).(*readerState)
	rec := st.rec

	// read the next record from *this* reader, and add it to the heap to be
	// considered on the next call to this method.
	nr, err := st.reader.Next()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("st.reader.Next: %w", err)
	}
	if nr != nil {
		st.rec = nr
		heap.Push(&m.h, st)
	}

	return rec, nil
}

type readerState struct {
	reader *Reader
	rec    *types.Record
}

type recHeap []*readerState

func (h recHeap) Len() int {
	return len(h)
}

func (h recHeap) Less(i, j int) bool {

	// when keys ar ethe same, sort by timestamp.
	if h[i].rec.Key == h[j].rec.Key {
		return h[i].rec.Timestamp.After(h[j].rec.Timestamp)
	}

	// otherwise just sort by key.
	return h[i].rec.Key < h[j].rec.Key
}

func (h recHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *recHeap) Push(x interface{}) {
	*h = append(*h, x.(*readerState))
}

func (h *recHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
