package mock

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockPutAndGet(t *testing.T) {
	ctx := context.Background()
	store := New()

	// Test basic put/get
	data := []byte("test data")
	err := store.Put(ctx, "testkey", bytes.NewReader(data))
	require.NoError(t, err)

	reader, err := store.Get(ctx, "testkey")
	require.NoError(t, err)

	gotData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data, gotData)
}

func TestMockGetRange(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	err := store.Put(ctx, "testkey2", bytes.NewReader(data))
	require.NoError(t, err)

	reader, err := store.GetRange(ctx, "testkey2", 3, 7)
	require.NoError(t, err)

	gotData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("defgh"), gotData)
}

func TestMockGetRangeZeroEnd(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("hello world")
	require.NoError(t, store.Put(ctx, "k", bytes.NewReader(data)))

	r, err := store.GetRange(ctx, "k", 6, 0)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), got)
}

func TestMockDelete(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("test data")
	err := store.Put(ctx, "testkey3", bytes.NewReader(data))
	require.NoError(t, err)

	err = store.Delete(ctx, "testkey3")
	require.NoError(t, err)

	_, err = store.Get(ctx, "testkey3")
	assert.Error(t, err)
}

func TestMockGetNonExistent(t *testing.T) {
	ctx := context.Background()
	store := New()

	_, err := store.Get(ctx, "nonexistent")
	assert.Error(t, err)
}
