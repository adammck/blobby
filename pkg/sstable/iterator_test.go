package sstable

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRangeIterator_EmptySSTable(t *testing.T) {
	ctx := context.Background()

	reader := createTestSSTable(t, nil)
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")
	defer iter.Close()

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
	require.Equal(t, "", iter.Key())
	require.Nil(t, iter.Value())

	timestampProvider := iter.(*rangeIterator)
	require.True(t, timestampProvider.Timestamp().IsZero())
}

func TestRangeIterator_SingleRecord(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "test-key", Document: []byte("test-doc"), Timestamp: now},
	})
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
	require.Equal(t, "test-key", iter.Key())
	require.Equal(t, []byte("test-doc"), iter.Value())

	// cast to access Timestamp method
	timestampProvider := iter.(*rangeIterator)
	require.Equal(t, now, timestampProvider.Timestamp())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_MultipleRecords(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "banana", Document: []byte("fruit2"), Timestamp: now.Add(1 * time.Second)},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")
	defer iter.Close()

	// should get all records in order
	require.True(t, iter.Next(ctx))
	require.Equal(t, "apple", iter.Key())
	require.Equal(t, []byte("fruit1"), iter.Value())
	timestampProvider := iter.(*rangeIterator)
	require.Equal(t, now, timestampProvider.Timestamp())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())
	require.Equal(t, []byte("fruit2"), iter.Value())
	timestampProvider = iter.(*rangeIterator)
	require.Equal(t, now.Add(1*time.Second), timestampProvider.Timestamp())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "cherry", iter.Key())
	require.Equal(t, []byte("fruit3"), iter.Value())
	timestampProvider = iter.(*rangeIterator)
	require.Equal(t, now.Add(2*time.Second), timestampProvider.Timestamp())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_RangeScanBasic(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "banana", Document: []byte("fruit2"), Timestamp: now.Add(1 * time.Second)},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
		{Key: "date", Document: []byte("fruit4"), Timestamp: now.Add(3 * time.Second)},
	})
	defer reader.Close()

	// scan range [b, d) - should get banana and cherry
	iter := NewRangeIterator(reader, "b", "d")
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())
	require.Equal(t, []byte("fruit2"), iter.Value())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "cherry", iter.Key())
	require.Equal(t, []byte("fruit3"), iter.Value())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_RangeScanStartOnly(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "banana", Document: []byte("fruit2"), Timestamp: now.Add(1 * time.Second)},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	// scan from "b" to end - should get banana and cherry
	iter := NewRangeIterator(reader, "b", "")
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "cherry", iter.Key())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_RangeScanEndOnly(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "banana", Document: []byte("fruit2"), Timestamp: now.Add(1 * time.Second)},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	// scan from start to "c" - should get apple and banana
	iter := NewRangeIterator(reader, "", "c")
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.Equal(t, "apple", iter.Key())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_EmptyRange(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	// scan range [b, c) - should be empty
	iter := NewRangeIterator(reader, "b", "c")
	defer iter.Close()

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_RangeExactBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "a", Document: []byte("val1"), Timestamp: now},
		{Key: "b", Document: []byte("val2"), Timestamp: now.Add(1 * time.Second)},
		{Key: "c", Document: []byte("val3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	// scan range [b, c) - should get only "b" (end is exclusive)
	iter := NewRangeIterator(reader, "b", "c")
	defer iter.Close()

	require.True(t, iter.Next(ctx))
	require.Equal(t, "b", iter.Key())
	require.Equal(t, []byte("val2"), iter.Value())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_TombstoneHandling(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "apple", Document: []byte("fruit1"), Timestamp: now},
		{Key: "banana", Document: nil, Timestamp: now.Add(1 * time.Second), Tombstone: true},
		{Key: "cherry", Document: []byte("fruit3"), Timestamp: now.Add(2 * time.Second)},
	})
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")
	defer iter.Close()

	// should return all records including tombstones (compound iterator filters them)
	require.True(t, iter.Next(ctx))
	require.Equal(t, "apple", iter.Key())
	require.Equal(t, []byte("fruit1"), iter.Value())

	require.True(t, iter.Next(ctx))
	require.Equal(t, "banana", iter.Key())
	require.Nil(t, iter.Value()) // tombstone has nil document

	require.True(t, iter.Next(ctx))
	require.Equal(t, "cherry", iter.Key())
	require.Equal(t, []byte("fruit3"), iter.Value())

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
}

func TestRangeIterator_StateAfterExhausted(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "only", Document: []byte("record"), Timestamp: now},
	})
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")
	defer iter.Close()

	// get the only record
	require.True(t, iter.Next(ctx))
	require.Equal(t, "only", iter.Key())

	// should be exhausted now
	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())

	// subsequent calls should continue to return false
	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())

	// accessor methods should still work on last record
	require.Equal(t, "only", iter.Key())
	require.Equal(t, []byte("record"), iter.Value())
	timestampProvider := iter.(*rangeIterator)
	require.Equal(t, now, timestampProvider.Timestamp())
}

func TestRangeIterator_CloseCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	reader := createTestSSTable(t, []types.Record{
		{Key: "test", Document: []byte("data"), Timestamp: now},
	})
	defer reader.Close()

	iter := NewRangeIterator(reader, "", "")

	// consume record
	require.True(t, iter.Next(ctx))
	require.Equal(t, "test", iter.Key())

	// close should succeed
	err := iter.Close()
	require.NoError(t, err)

	// multiple closes should be safe (depends on reader implementation)
	err = iter.Close()
	require.NoError(t, err)
}

// helper function to create test sstable
func createTestSSTable(t *testing.T, records []types.Record) *Reader {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))

	for _, record := range records {
		_, err := record.Write(&buf)
		require.NoError(t, err)
	}

	reader, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)

	return reader
}
