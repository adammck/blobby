package index

import (
	"sort"

	"github.com/adammck/blobby/pkg/api"
)

type Range struct {
	First int64
	Last  int64
}

type Indexer struct {
	entries []api.IndexEntry
}

func New(idx api.Index) *Indexer {
	if len(idx) == 0 {
		return &Indexer{entries: []api.IndexEntry{}}
	}

	entries := make([]api.IndexEntry, len(idx))
	copy(entries, idx)

	// should already be sorted, but no need to assume that.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return &Indexer{entries: entries}
}

func (idx *Indexer) Lookup(key string) Range {
	if len(idx.entries) == 0 {
		return Range{First: 0, Last: -1}
	}

	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Key > key
	}) - 1

	if i < 0 {
		return Range{First: 0, Last: idx.entries[0].Offset - 1}
	}

	first := idx.entries[i].Offset
	var last int64 = -1

	if i+1 < len(idx.entries) {
		last = idx.entries[i+1].Offset - 1
	}

	return Range{First: first, Last: last}
}
