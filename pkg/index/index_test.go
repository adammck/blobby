package index

import (
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestIndexer(t *testing.T) {
	idx := []api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "banana", Offset: 200},
		{Key: "cherry", Offset: 300},
		{Key: "date", Offset: 400},
		{Key: "elderberry", Offset: 500},
	}

	indexer := New(idx)

	tests := []struct {
		name     string
		key      string
		expected Range
	}{
		{
			name:     "exact match",
			key:      "banana",
			expected: Range{First: 200, Last: 299},
		},
		{
			name:     "between entries",
			key:      "blackberry",
			expected: Range{First: 200, Last: 299},
		},
		{
			name:     "before first entry",
			key:      "aardvark",
			expected: Range{First: 0, Last: 99},
		},
		{
			name:     "after last entry",
			key:      "zebra",
			expected: Range{First: 500, Last: -1},
		},
		{
			name:     "first entry",
			key:      "apple",
			expected: Range{First: 100, Last: 199},
		},
		{
			name:     "last entry",
			key:      "elderberry",
			expected: Range{First: 500, Last: -1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := indexer.Lookup(tc.key)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestEmptyIndex(t *testing.T) {
	indexer := New([]api.IndexEntry{})
	result := indexer.Lookup("anything")
	require.Equal(t, Range{First: 0, Last: -1}, result)
}

func TestUnsortedIndex(t *testing.T) {
	idx := []api.IndexEntry{
		{Key: "date", Offset: 400},
		{Key: "apple", Offset: 100},
		{Key: "elderberry", Offset: 500},
		{Key: "cherry", Offset: 300},
		{Key: "banana", Offset: 200},
	}

	indexer := New(idx)

	result := indexer.Lookup("cherry")
	require.Equal(t, Range{First: 300, Last: 399}, result)
}
