package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())

	store := New(env.MongoURL())
	err := store.Init(ctx)
	require.NoError(t, err)

	// second call fails on an existing db.
	err = store.Init(ctx)
	assert.Error(t, err)
}

func setup(t *testing.T) (context.Context, *Store) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())

	store := New(env.MongoURL())
	err := store.Init(ctx)
	require.NoError(t, err)

	return ctx, store
}

func TestEmpty(t *testing.T) {
	ctx, store := setup(t)

	metas, err := store.GetContaining(ctx, "x")
	require.NoError(t, err)
	assert.Empty(t, metas)
}

func TestBasicUsage(t *testing.T) {
	ctx, store := setup(t)

	m1 := &api.BlobMeta{
		MinKey:  "a",
		MaxKey:  "c",
		MinTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Count:   10,
		Size:    1000,
		Created: time.Now().UTC(),
	}

	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	// in range
	metas, err := store.GetContaining(ctx, "b")
	require.NoError(t, err)
	require.Len(t, metas, 1)
	assert.Equal(t, m1.MinKey, metas[0].MinKey)
	assert.Equal(t, m1.MaxKey, metas[0].MaxKey)

	// not in range
	metas, err = store.GetContaining(ctx, "d")
	require.NoError(t, err)
	assert.Empty(t, metas)
}

func TestSortByMaxTime(t *testing.T) {
	ctx, store := setup(t)

	now := time.Now().UTC()
	m1 := &api.BlobMeta{
		MinKey:  "a",
		MaxKey:  "c",
		MinTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Count:   10,
		Size:    1000,
		Created: now.Add(-time.Hour), // older creation time
	}
	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	m2 := &api.BlobMeta{
		MinKey:  "b",
		MaxKey:  "d",
		MinTime: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
		Count:   10,
		Size:    1000,
		Created: now,
	}
	err = store.Insert(ctx, m2)
	require.NoError(t, err)

	// should be sorted by max_time desc
	metas, err := store.GetContaining(ctx, "b")
	require.NoError(t, err)
	require.Len(t, metas, 2)
	assert.Equal(t, m2.MaxTime, metas[0].MaxTime) // newer max time first
	assert.Equal(t, m1.MaxTime, metas[1].MaxTime)
}

func TestSortByCreatedForSameMaxTime(t *testing.T) {
	ctx, store := setup(t)

	now := time.Now().UTC()
	sameTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	m1 := &api.BlobMeta{
		MinKey:  "a",
		MaxKey:  "c",
		MinTime: sameTime,
		MaxTime: sameTime,
		Count:   10,
		Size:    1000,
		Created: now.Add(-time.Hour), // older creation time
	}
	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	m2 := &api.BlobMeta{
		MinKey:  "b",
		MaxKey:  "d",
		MinTime: sameTime,
		MaxTime: sameTime,
		Count:   10,
		Size:    1000,
		Created: now, // newer creation time
	}
	err = store.Insert(ctx, m2)
	require.NoError(t, err)

	// should be sorted by created desc when max_time is equal
	metas, err := store.GetContaining(ctx, "b")
	require.NoError(t, err)
	require.Len(t, metas, 2)
	assert.Equal(t, m2.Created.Unix(), metas[0].Created.Unix()) // newer created first
	assert.Equal(t, m1.Created.Unix(), metas[1].Created.Unix())
}

func TestBoundaryCases(t *testing.T) {
	ctx, store := setup(t)

	now := time.Now().UTC()
	m1 := &api.BlobMeta{
		MinKey:  "m",
		MaxKey:  "n",
		MinTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Count:   10,
		Size:    1000,
		Created: now,
	}
	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	// on min key
	metas, err := store.GetContaining(ctx, "m")
	require.NoError(t, err)
	require.Len(t, metas, 1)

	// on max key
	metas, err = store.GetContaining(ctx, "n")
	require.NoError(t, err)
	require.Len(t, metas, 1)
}
