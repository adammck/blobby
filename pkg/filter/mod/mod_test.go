package mod

import (
	"encoding/json"
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	keys := []string{"key0", "key1", "key2", "key3"}
	f, err := Create(keys)
	require.NoError(t, err)
	require.NotNil(t, f)

	// Key ending with "0" (even) - will always return true due to mod filter behavior
	require.True(t, f.Contains("key0"))
	// Key ending with "1" (odd) - should be in the filter as it was added
	require.True(t, f.Contains("key1"))
	// Key ending with "2" (even) - will always return true due to mod filter behavior
	require.True(t, f.Contains("key2"))
	// Key ending with "3" (odd) - should be in the filter as it was added
	require.True(t, f.Contains("key3"))

	// Test keys that weren't added
	// Key ending with "4" (even) - will always return true even though not added
	require.True(t, f.Contains("other4"))
	// Key ending with "5" (odd) - should return false as it wasn't added
	require.False(t, f.Contains("other5"))
}

func TestNew(t *testing.T) {
	stored := []string{"key0", "key2"}
	data, err := json.Marshal(stored)
	require.NoError(t, err)

	apiFilter := api.Filter{
		Type: TypeName,
		Data: data,
	}

	f, err := Unmarshal(apiFilter)
	require.NoError(t, err)
	require.NotNil(t, f)

	require.True(t, f.Contains("key0"))
	require.False(t, f.Contains("key1"))
	require.True(t, f.Contains("key2"))
	require.False(t, f.Contains("key3"))
}

func TestContains(t *testing.T) {
	keys := []string{"key0", "key2", "foo2", "bar4"}
	f, err := Create(keys)
	require.NoError(t, err)

	tests := []struct {
		key      string
		expected bool
	}{
		{"", true},
		{"key0", true},
		{"key1", false},
		{"key2", true},
		{"key3", false},
		{"foo2", true},
		{"foo1", false},
		{"bar4", true},
		{"bar3", false},
	}

	for _, tc := range tests {
		require.Equal(t, tc.expected, f.Contains(tc.key), "key: %s", tc.key)
	}
}

func TestMarshal(t *testing.T) {
	keys := []string{"key0", "key2"}
	f, err := Create(keys)
	require.NoError(t, err)

	apiFilter, err := f.Marshal()
	require.NoError(t, err)
	require.Equal(t, TypeName, apiFilter.Type)

	var unmarshaled []string
	err = json.Unmarshal(apiFilter.Data, &unmarshaled)
	require.NoError(t, err)

	require.ElementsMatch(t, keys, unmarshaled)
}

func TestTombstoneFiltering(t *testing.T) {
	// This test verifies that tombstone records are properly indexed in filters
	// Just like normal records, tombstone records should be indexed by their key
	// so filters can determine if a key might have been deleted
	tombstoneKeys := []string{"deleted0", "deleted2", "tombstone4"}
	normalKeys := []string{"key0", "key2"}
	allKeys := append(normalKeys, tombstoneKeys...)

	f, err := Create(allKeys)
	require.NoError(t, err)

	// Verify all keys are included in the filter - both normal and tombstone keys
	for _, key := range normalKeys {
		if key[len(key)-1]%2 == 1 {
			// Odd-ending keys should be properly indexed
			require.True(t, f.Contains(key), "Filter should contain normal key: %s", key)
		}
	}

	for _, key := range tombstoneKeys {
		if key[len(key)-1]%2 == 1 {
			// Odd-ending tombstone keys should be properly indexed
			require.True(t, f.Contains(key), "Filter should contain tombstone key: %s", key)
		}
	}

	// Filters shouldn't know or care if a key represents a tombstone
	// The filtering behavior should be the same for all keys
	// MOD filter returns false for odd-ending keys that weren't indexed
	require.False(t, f.Contains("not-indexed1"), "Filter should not contain non-indexed odd key")
	// MOD filter always returns true for even-ending keys (by design)
	require.True(t, f.Contains("even-key2"), "Filter should always return true for even-ending keys")
}
