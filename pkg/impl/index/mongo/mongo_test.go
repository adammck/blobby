package mongo

import (
	"context"
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *IndexStore) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	db := env.Mongo().Database("blobby")
	store := New(db)
	err := store.Init(ctx)
	require.NoError(t, err)
	return ctx, store
}

func TestStoreAndGetIndex(t *testing.T) {
	ctx, store := setup(t)
	fn := "test.sstable"
	entries := []api.IndexEntry{
		{Key: "a", Offset: 100},
		{Key: "b", Offset: 200},
		{Key: "c", Offset: 300},
	}

	err := store.StoreIndex(ctx, fn, entries)
	require.NoError(t, err)

	retrieved, err := store.GetIndex(ctx, fn)
	require.NoError(t, err)
	require.Len(t, retrieved, 3)

	require.Equal(t, "a", retrieved[0].Key)
	require.Equal(t, int64(100), retrieved[0].Offset)
	require.Equal(t, "b", retrieved[1].Key)
	require.Equal(t, int64(200), retrieved[1].Offset)
	require.Equal(t, "c", retrieved[2].Key)
	require.Equal(t, int64(300), retrieved[2].Offset)
}

func TestUpdateIndex(t *testing.T) {
	ctx, store := setup(t)
	fn := "test.sstable"
	entries := []api.IndexEntry{
		{Key: "a", Offset: 100},
		{Key: "b", Offset: 200},
	}

	err := store.StoreIndex(ctx, fn, entries)
	require.NoError(t, err)

	updatedEntries := []api.IndexEntry{
		{Key: "c", Offset: 300},
		{Key: "d", Offset: 400},
	}

	err = store.StoreIndex(ctx, fn, updatedEntries)
	require.NoError(t, err)

	retrieved, err := store.GetIndex(ctx, fn)
	require.NoError(t, err)
	require.Len(t, retrieved, 2)
	require.Equal(t, "c", retrieved[0].Key)
	require.Equal(t, "d", retrieved[1].Key)
}

func TestDeleteIndex(t *testing.T) {
	ctx, store := setup(t)
	fn := "test.sstable"
	entries := []api.IndexEntry{
		{Key: "a", Offset: 100},
		{Key: "b", Offset: 200},
	}

	err := store.StoreIndex(ctx, fn, entries)
	require.NoError(t, err)

	err = store.DeleteIndex(ctx, fn)
	require.NoError(t, err)

	_, err = store.GetIndex(ctx, fn)
	require.Error(t, err)
}

func TestGetNonExistentIndex(t *testing.T) {
	ctx, store := setup(t)

	_, err := store.GetIndex(ctx, "nonexistent.sstable")
	require.Error(t, err)
}
