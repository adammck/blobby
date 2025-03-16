package index

import (
	"fmt"
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

func New(idx []api.IndexEntry) *Indexer {
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

func (idx *Indexer) Lookup(key string) (*Range, error) {
	if len(idx.entries) == 0 {
		return nil, nil
	}

	// find the first range *after* the key. beware: yields len(entries) if the
	// key is greater than the last entry, which is not a valid index!
	after := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Key > key
	})

	// sanity check: it's not valid for the first entry to begin *after* the
	// key, because we always output an index entry before the first record, so
	// seekers can skip the header. if we see this, the caller has probably not
	// checked the key range in the metadata, so is looking for a key in an
	// sstable which couldn't possibly contain it. or maybe index is corrupted.
	// either way, we can't do anything useful.
	if after == 0 {
		return nil, fmt.Errorf("first index entry is after key %q", key)
	}

	// this part is confusing. we need to find the first entry which *might*
	// contain the key. remember that the same key may appear many times (with
	// different timestamps), and the index entry might not be pointing to the
	// first one. so we need to walk backwards until we find a range with a
	// lower key.
	first := after - 1
	for {
		if first == 0 {
			break
		}
		if idx.entries[first].Key < key {
			break
		}
		first--
	}

	// if the key was after the key of the last entry, then we can't provide a
	// byte offset. the caller has to just read to the end of the file. but if
	// we found an entry after the key, then we can stop reading there.
	var last int64
	if after < len(idx.entries) {
		last = idx.entries[after].Offset - 1
	}

	// otherwise, the key is in the range preceding the one we found.
	// the last offset is the offset of the next entry, minus 1.
	return &Range{
		First: idx.entries[first].Offset,
		Last:  last,
	}, nil
}
