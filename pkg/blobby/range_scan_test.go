package blobby

import (
	"context"
	"testing"

	"github.com/adammck/blobby/pkg/blobby/testutil"
	"github.com/stretchr/testify/require"
)

func TestRangeScanBasic(t *testing.T) {
	ctx := context.Background()
	fake := testutil.NewFakeBlobby()

	// put some test data
	_, err := fake.Put(ctx, "apple", []byte("fruit1"))
	require.NoError(t, err)

	_, err = fake.Put(ctx, "banana", []byte("fruit2"))
	require.NoError(t, err)

	_, err = fake.Put(ctx, "cherry", []byte("fruit3"))
	require.NoError(t, err)

	_, err = fake.Put(ctx, "date", []byte("fruit4"))
	require.NoError(t, err)

	// test range scan
	iter, stats, err := fake.RangeScan(ctx, "b", "d")
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.NotNil(t, stats)
	defer iter.Close()

	// should get banana and cherry (range is [start, end))
	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())
	require.Equal(t, []byte("fruit2"), iter.Value())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "cherry", iter.Key())
	require.Equal(t, []byte("fruit3"), iter.Value())

	// should be done
	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeScanEmpty(t *testing.T) {
	ctx := context.Background()
	fake := testutil.NewFakeBlobby()

	// test empty range
	iter, stats, err := fake.RangeScan(ctx, "x", "z")
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.NotNil(t, stats)
	defer iter.Close()

	// should be empty
	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeScanAllKeys(t *testing.T) {
	ctx := context.Background()
	fake := testutil.NewFakeBlobby()

	// put some test data
	_, err := fake.Put(ctx, "key1", []byte("val1"))
	require.NoError(t, err)

	_, err = fake.Put(ctx, "key2", []byte("val2"))
	require.NoError(t, err)

	// test scanning all keys (empty start and end)
	iter, stats, err := fake.RangeScan(ctx, "", "")
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.NotNil(t, stats)
	defer iter.Close()

	count := 0
	for iter.Next(ctx) {
		count++
		require.NotEmpty(t, iter.Key())
		require.NotEmpty(t, iter.Value())
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 2, count)
}

func TestRangeScanWithTombstones(t *testing.T) {
	ctx := context.Background()
	fake := testutil.NewFakeBlobby()

	// put and delete a key
	_, err := fake.Put(ctx, "key1", []byte("val1"))
	require.NoError(t, err)

	_, err = fake.Put(ctx, "key2", []byte("val2"))
	require.NoError(t, err)

	_, err = fake.Delete(ctx, "key1")
	require.NoError(t, err)

	// scan should only return key2
	iter, _, err := fake.RangeScan(ctx, "", "")
	require.NoError(t, err)
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.Equal(t, "key2", iter.Key())
	require.Equal(t, []byte("val2"), iter.Value())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}
