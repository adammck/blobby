package index

import (
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestIndexer(t *testing.T) {
	indexer := New([]api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "banana", Offset: 200},
		{Key: "cherry", Offset: 300},
		{Key: "date", Offset: 400},
		{Key: "elderberry", Offset: 500},
	})

	tests := []struct {
		name        string
		key         string
		expected    *Range
		expectError bool
	}{
		{
			// most common case
			name:        "between entries",
			key:         "blackberry",
			expected:    &Range{First: 200, Last: 299},
			expectError: false,
		},
		{
			// unfortunate case where the key is the one in the index entry; we
			// have to check the block before, too, in case the record which the
			// entry points to is in the middle a series with the same keys but
			// different timestamps.
			name:        "exact match",
			key:         "banana",
			expected:    &Range{First: 100, Last: 299},
			expectError: false,
		},
		{
			// should never happen
			name:        "before first entry",
			key:         "aardvark",
			expected:    nil,
			expectError: true,
		},
		{
			// can happen! we currently leave no trailer, and assume that the
			// caller validates the range of keys in the sstable.
			name:        "after last entry",
			key:         "zebra",
			expected:    &Range{First: 500, Last: 0},
			expectError: false,
		},
		{
			name:        "first entry",
			key:         "apple",
			expected:    &Range{First: 100, Last: 199},
			expectError: false,
		},
		{
			name:        "last entry",
			key:         "elderberry",
			expected:    &Range{First: 400, Last: 0},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rng, err := indexer.Lookup(tc.key)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, rng)
			}
		})
	}
}

func TestEmptyIndex(t *testing.T) {
	indexer := New([]api.IndexEntry{})
	rng, err := indexer.Lookup("anything")
	require.NoError(t, err)
	require.Nil(t, rng)
}

func TestUnsortedIndex(t *testing.T) {
	indexer := New([]api.IndexEntry{
		{Key: "date", Offset: 400},
		{Key: "apple", Offset: 100},
		{Key: "elderberry", Offset: 500},
		{Key: "cherry", Offset: 300},
		{Key: "banana", Offset: 200},
	})

	rng, err := indexer.Lookup("cantaloupe")
	require.NoError(t, err)
	require.Equal(t, &Range{First: 200, Last: 299}, rng)
}

func TestDuplicateKeysStart(t *testing.T) {
	indexer := New([]api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "apple", Offset: 200},
		{Key: "apple", Offset: 300},
		{Key: "banana", Offset: 400},
		{Key: "cherry", Offset: 500},
	})

	rng, err := indexer.Lookup("apple")
	require.NoError(t, err)
	require.Equal(t, &Range{First: 100, Last: 399}, rng)
}

func TestDuplicateKeysMid(t *testing.T) {
	idx := []api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "banana", Offset: 200},
		{Key: "banana", Offset: 300},
		{Key: "banana", Offset: 400},
		{Key: "cherry", Offset: 500},
	}

	indexer := New(idx)
	rng, err := indexer.Lookup("banana")
	require.NoError(t, err)
	require.Equal(t, &Range{First: 100, Last: 499}, rng)
}

func TestDuplicateKeysEnd(t *testing.T) {
	indexer := New([]api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "banana", Offset: 200},
		{Key: "cherry", Offset: 300},
		{Key: "cherry", Offset: 400},
		{Key: "cherry", Offset: 500},
	})

	rng, err := indexer.Lookup("cherry")
	require.NoError(t, err)
	require.Equal(t, &Range{First: 200, Last: 0}, rng)
}

func TestDuplicateKeysAll(t *testing.T) {
	indexer := New([]api.IndexEntry{
		{Key: "apple", Offset: 100},
		{Key: "apple", Offset: 200},
		{Key: "apple", Offset: 300},
		{Key: "apple", Offset: 400},
		{Key: "apple", Offset: 500},
	})

	rng, err := indexer.Lookup("apple")
	require.NoError(t, err)
	require.Equal(t, &Range{First: 100, Last: 0}, rng)
}
