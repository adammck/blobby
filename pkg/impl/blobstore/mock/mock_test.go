package mock

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("test data")
	err := store.Put(ctx, "testkey", bytes.NewReader(data))
	require.NoError(t, err)

	r, err := store.Get(ctx, "testkey")
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestGetRange(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	err := store.Put(ctx, "testkey2", bytes.NewReader(data))
	require.NoError(t, err)

	r, err := store.GetRange(ctx, "testkey2", 3, 7)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("defgh"), got)
}

func TestGetRangeZeroLast(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("hello world")
	require.NoError(t, store.Put(ctx, "k", bytes.NewReader(data)))

	r, err := store.GetRange(ctx, "k", 6, 0)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), got)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	store := New()

	data := []byte("test")
	err := store.Put(ctx, "k", bytes.NewReader(data))
	require.NoError(t, err)

	err = store.Delete(ctx, "k")
	require.NoError(t, err)

	_, err = store.Get(ctx, "k")
	require.Error(t, err)
}

func TestGetNonExistent(t *testing.T) {
	ctx := context.Background()
	store := New()

	_, err := store.Get(ctx, "nonexistent")
	require.Error(t, err)
}
