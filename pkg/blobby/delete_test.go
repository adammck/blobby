package blobby

import (
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	c := clockwork.NewRealClock()
	ctx, _, b := setup(t, c)

	// Test case 1: Delete an existing key
	// First put some data
	dest, err := b.Put(ctx, "key1", []byte("value1"))
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it exists
	val, _, err := b.Get(ctx, "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	// Delete it
	dest, err = b.Delete(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it no longer exists
	val, _, err = b.Get(ctx, "key1")
	require.NoError(t, err)
	require.Nil(t, val)

	// Test case 2: Delete a non-existent key
	dest, err = b.Delete(ctx, "nonexistent")
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it doesn't exist
	val, _, err = b.Get(ctx, "nonexistent")
	require.NoError(t, err)
	require.Nil(t, val)

	// Test case 3: Delete an already-deleted key
	dest, err = b.Delete(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it still doesn't exist
	val, _, err = b.Get(ctx, "key1")
	require.NoError(t, err)
	require.Nil(t, val)

	// Test case 4: Delete then re-add a key
	// First delete
	dest, err = b.Delete(ctx, "key2")
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Now add
	dest, err = b.Put(ctx, "key2", []byte("new_value"))
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it exists with new value
	val, _, err = b.Get(ctx, "key2")
	require.NoError(t, err)
	require.Equal(t, []byte("new_value"), val)

	// Test case 5: Delete key that exists in multiple versions
	// Add initial version
	dest, err = b.Put(ctx, "versioned", []byte("v1"))
	require.NoError(t, err)

	// Add second version
	dest, err = b.Put(ctx, "versioned", []byte("v2"))
	require.NoError(t, err)

	// Delete it
	dest, err = b.Delete(ctx, "versioned")
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Verify it no longer exists
	val, _, err = b.Get(ctx, "versioned")
	require.NoError(t, err)
	require.Nil(t, val)
}
