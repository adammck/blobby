package memtable

import (
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestMemtableIterator_EmptyResults(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	require.False(t, it.Next(ctx))
	require.NoError(t, it.Err())
	require.Equal(t, "", it.Key())
	require.Nil(t, it.Value())
	require.True(t, it.CurrentTimestamp().IsZero())

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_SingleRecord(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	testKey := "test-key"
	testDoc := []byte("test-document")

	_, err = mt.Put(ctx, testKey, testDoc)
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	require.True(t, it.Next(ctx))
	require.NoError(t, it.Err())
	require.Equal(t, testKey, it.Key())
	require.Equal(t, testDoc, it.Value())
	require.False(t, it.CurrentTimestamp().IsZero())

	require.False(t, it.Next(ctx))
	require.NoError(t, it.Err())

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_MultipleRecords(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	records := []struct {
		key string
		doc []byte
	}{
		{"key1", []byte("doc1")},
		{"key2", []byte("doc2")},
		{"key3", []byte("doc3")},
	}

	for _, rec := range records {
		_, err = mt.Put(ctx, rec.key, rec.doc)
		require.NoError(t, err)
		c.Advance(time.Second)
	}

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	seen := make(map[string][]byte)
	count := 0

	for it.Next(ctx) {
		require.NoError(t, it.Err())
		seen[it.Key()] = it.Value()
		require.False(t, it.CurrentTimestamp().IsZero())
		count++
	}

	require.NoError(t, it.Err())
	require.Equal(t, len(records), count)

	for _, rec := range records {
		require.Equal(t, rec.doc, seen[rec.key])
	}

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_AccessorsBeforeNext(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	require.Equal(t, "", it.Key())
	require.Nil(t, it.Value())
	require.True(t, it.CurrentTimestamp().IsZero())
	require.NoError(t, it.Err())

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_NextAfterError(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
		err:    mongo.ErrNoDocuments,
	}

	require.False(t, it.Next(ctx))
	require.Equal(t, mongo.ErrNoDocuments, it.Err())

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_CursorDecodeError(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	db, err := mt.GetMongo(ctx)
	require.NoError(t, err)

	coll := db.Collection("invalid-collection")
	
	// insert a document that can't be decoded as types.Record
	_, err = coll.InsertOne(ctx, bson.M{
		"key": 123,  // should be string
		"ts": "invalid-time", // should be time.Time
		"doc": "should-be-bytes",
	})
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	require.False(t, it.Next(ctx))
	require.Error(t, it.Err())

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_CloseError(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	it := &memtableIterator{
		cursor: cursor,
		ctx:    canceledCtx,
	}

	err = it.Close()
	require.NoError(t, err)
}

func TestMemtableIterator_WithTombstones(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	_, err = mt.Put(ctx, "alive", []byte("data"))
	require.NoError(t, err)

	_, err = mt.PutRecord(ctx, &types.Record{
		Key:       "deleted",
		Tombstone: true,
	})
	require.NoError(t, err)

	coll, err := mt.activeCollection(ctx)
	require.NoError(t, err)

	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err)

	it := &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}

	records := make(map[string]bool)
	for it.Next(ctx) {
		require.NoError(t, it.Err())
		key := it.Key()
		if key == "alive" {
			require.NotNil(t, it.Value())
			records[key] = false
		} else if key == "deleted" {
			records[key] = true
		}
		require.False(t, it.CurrentTimestamp().IsZero())
	}

	require.NoError(t, it.Err())
	require.Contains(t, records, "alive")
	require.Contains(t, records, "deleted")

	err = it.Close()
	require.NoError(t, err)
}
