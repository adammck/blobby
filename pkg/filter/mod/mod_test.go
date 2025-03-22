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

	require.True(t, f.Contains("key0"))
	require.False(t, f.Contains("key1"))
	require.True(t, f.Contains("key2"))
	require.False(t, f.Contains("key3"))
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
		{"key0", true},
		{"key1", false},
		{"key2", true},
		{"key3", false},
		{"foo2", true},
		{"foo1", false},
		{"bar4", true},
		{"bar3", false},
		{"", false},
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
