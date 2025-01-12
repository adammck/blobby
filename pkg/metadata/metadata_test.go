package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	ctx := context.Background()
	env := testutil.SetupTest(ctx, t)

	store := New(env.MongoURL)
	err := store.Init(ctx)
	require.NoError(t, err)

	// second call fails on an existing db.
	err = store.Init(ctx)
	assert.Error(t, err)
}

func setup(t *testing.T) (context.Context, *Store) {
	ctx := context.Background()
	env := testutil.SetupTest(ctx, t)

	store := New(env.MongoURL)
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

	m1 := &sstable.Meta{
		MinKey:  "a",
		MaxKey:  "c",
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

func TestOverlappingSSTables(t *testing.T) {
	ctx, store := setup(t)

	m1 := &sstable.Meta{
		MinKey:  "a",
		MaxKey:  "c",
		Count:   10,
		Size:    1000,
		Created: time.Now().UTC(),
	}
	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	m2 := &sstable.Meta{
		MinKey:  "b",
		MaxKey:  "d",
		Count:   10,
		Size:    1000,
		Created: time.Now().UTC().Add(time.Hour),
	}
	err = store.Insert(ctx, m2)
	require.NoError(t, err)

	// in range of both
	metas, err := store.GetContaining(ctx, "b")
	require.NoError(t, err)
	require.Len(t, metas, 2)

	// sorted by creation time
	assert.Equal(t, m2.Created.Unix(), metas[0].Created.Unix())
	assert.Equal(t, m1.Created.Unix(), metas[1].Created.Unix())
}

func TestBoundaryCases(t *testing.T) {
	ctx, store := setup(t)

	m1 := &sstable.Meta{
		MinKey:  "m",
		MaxKey:  "n",
		Count:   10,
		Size:    1000,
		Created: time.Now().UTC(),
	}
	err := store.Insert(ctx, m1)
	require.NoError(t, err)

	// on min key
	metas, err := store.GetContaining(ctx, "m")
	require.NoError(t, err)
	require.Len(t, metas, 1)

	// on max
	metas, err = store.GetContaining(ctx, "n")
	require.NoError(t, err)
	require.Len(t, metas, 1)
}
