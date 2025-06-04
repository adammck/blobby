package memtable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/adammck/blobby/pkg/types"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMemtableIterator_EmptyResults(t *testing.T) {
	it, ctx := createEmptyTestIterator(t)

	require.False(t, it.Next(ctx))
	require.NoError(t, it.Err())
	require.Equal(t, "", it.Key())
	require.Nil(t, it.Value())
	require.True(t, it.Timestamp().IsZero())

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_SingleRecord(t *testing.T) {
	now := time.Now()
	it, ctx := createTestIterator(t, []types.Record{
		{
			Key:       "test-key",
			Document:  []byte("test-doc"),
			Timestamp: now,
		},
	})

	require.True(t, it.Next(ctx))
	require.NoError(t, it.Err())
	require.Equal(t, "test-key", it.Key())
	require.Equal(t, []byte("test-doc"), it.Value())

	if useMockCursors() {
		require.Equal(t, now, it.Timestamp())
	} else {
		require.False(t, it.Timestamp().IsZero())
	}

	require.False(t, it.Next(ctx))
	require.NoError(t, it.Err())

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_MultipleRecords(t *testing.T) {
	it, ctx := createTestIterator(t, []types.Record{
		{Key: "key1", Document: []byte("doc1"), Timestamp: time.Now()},
		{Key: "key2", Document: []byte("doc2"), Timestamp: time.Now().Add(time.Second)},
		{Key: "key3", Document: []byte("doc3"), Timestamp: time.Now().Add(2 * time.Second)},
	})

	seen := make(map[string][]byte)
	count := 0

	for it.Next(ctx) {
		require.NoError(t, it.Err())
		seen[it.Key()] = it.Value()
		require.False(t, it.Timestamp().IsZero())
		count++
	}

	require.NoError(t, it.Err())
	require.Equal(t, 3, count)
	require.Equal(t, []byte("doc1"), seen["key1"])
	require.Equal(t, []byte("doc2"), seen["key2"])
	require.Equal(t, []byte("doc3"), seen["key3"])

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_AccessorsBeforeNext(t *testing.T) {
	it, _ := createEmptyTestIterator(t)

	require.Equal(t, "", it.Key())
	require.Nil(t, it.Value())
	require.True(t, it.Timestamp().IsZero())
	require.NoError(t, it.Err())

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_NextAfterError(t *testing.T) {
	it, ctx := createTestIteratorWithPresetError(t, mongo.ErrNoDocuments)

	require.False(t, it.Next(ctx))
	require.Equal(t, mongo.ErrNoDocuments, it.Err())

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_CursorDecodeError(t *testing.T) {
	it, ctx := createTestIteratorWithError(t)

	require.False(t, it.Next(ctx))
	require.Error(t, it.Err())

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_CloseError(t *testing.T) {
	it, _ := createTestIteratorWithCanceledContext(t)

	err := it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_WithTombstones(t *testing.T) {
	it, ctx := createTestIterator(t, []types.Record{
		{Key: "key1", Document: []byte("data1")},
		{Key: "key2", Tombstone: true},
		{Key: "key3", Document: []byte("data3")},
	})

	seen := make(map[string][]byte)
	tombstones := make(map[string]bool)
	count := 0

	for it.Next(ctx) {
		require.NoError(t, it.Err())
		key := it.Key()
		value := it.Value()

		seen[key] = value
		tombstones[key] = (len(value) == 0)
		require.False(t, it.Timestamp().IsZero())
		count++
	}

	require.NoError(t, it.Err())
	require.Equal(t, 3, count)

	require.Contains(t, seen, "key1")
	require.Equal(t, []byte("data1"), seen["key1"])
	require.False(t, tombstones["key1"])

	require.Contains(t, seen, "key2")
	require.Empty(t, seen["key2"])
	require.True(t, tombstones["key2"])

	require.Contains(t, seen, "key3")
	require.Equal(t, []byte("data3"), seen["key3"])
	require.False(t, tombstones["key3"])

	err := it.Close()
	require.NoError(t, err)
}

func useMockCursors() bool {
	return os.Getenv("BLOBBY_TEST_MONGO") != "1"
}

func createTestIterator(t *testing.T, records []types.Record) (*memtableIterator, context.Context) {
	return createTestIteratorWithContext(t, records, context.Background())
}

func createEmptyTestIterator(t *testing.T) (*memtableIterator, context.Context) {
	return createTestIterator(t, []types.Record{})
}

func createTestIteratorWithError(t *testing.T) (*memtableIterator, context.Context) {
	ctx := context.Background()

	if useMockCursors() {
		return &memtableIterator{
			cursor: newFailingMockCursor(errors.New("decode failed")),
			ctx:    ctx,
		}, ctx
	}

	t.Skip("error injection test only supported with mock cursors")
	return nil, nil
}

func createTestIteratorWithPresetError(t *testing.T, err error) (*memtableIterator, context.Context) {
	it, ctx := createTestIterator(t, []types.Record{})
	it.err = err
	return it, ctx
}

func createTestIteratorWithCanceledContext(t *testing.T) (*memtableIterator, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return createTestIteratorWithContext(t, []types.Record{}, ctx)
}

func createTestIteratorWithContext(t *testing.T, records []types.Record, iterCtx context.Context) (*memtableIterator, context.Context) {
	ctx := context.Background()

	if useMockCursors() {
		return &memtableIterator{
			cursor: newMockCursor(records),
			ctx:    iterCtx,
		}, iterCtx
	}

	env := testdeps.New(ctx, t, testdeps.WithMongo())

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(env.MongoURL()))
	require.NoError(t, err)

	db := client.Database("blobby_test")
	collName := fmt.Sprintf("test_%d", time.Now().UnixNano())
	coll := db.Collection(collName)

	// clean up collection after test
	t.Cleanup(func() {
		coll.Drop(context.Background())
		client.Disconnect(context.Background())
	})

	// insert records directly as bson documents
	if len(records) > 0 {
		docs := make([]any, len(records))
		baseTime := time.Now().UTC().Truncate(time.Millisecond)

		for i, record := range records {
			ts := baseTime.Add(time.Duration(i) * time.Millisecond)
			if !record.Timestamp.IsZero() {
				ts = record.Timestamp.UTC().Truncate(time.Millisecond)
			}

			doc := bson.M{
				"key": record.Key,
				"ts":  ts,
			}

			if record.Tombstone {
				doc["tombstone"] = true
				doc["doc"] = []byte{}
			} else {
				doc["doc"] = record.Document
			}

			docs[i] = doc
		}

		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err)
	}

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	return &memtableIterator{
		cursor: cursor,
		ctx:    iterCtx,
	}, iterCtx
}
