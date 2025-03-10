package memtable

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestSwap(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	// Get initial memtable name
	mtn1, err := getCurrentMemtableName(ctx, t, mt)
	require.NoError(t, err)

	// Write to initial table
	dest1, err := mt.Put(ctx, "k1", []byte("v1"))
	require.NoError(t, err)
	require.Equal(t, mtn1, dest1)

	// Advance the clock before swap to ensure unique timestamp
	c.Advance(1 * time.Second)

	// Swap to create a new memtable
	hOld, hNew, err := mt.Rotate(ctx)
	require.NoError(t, err)
	require.Equal(t, mtn1, hOld.Name())
	require.NotEqual(t, mtn1, hNew.Name())
	require.True(t, strings.HasPrefix(hNew.Name(), "mt_"))

	// Verify the new memtable is now active
	mtn2, err := getCurrentMemtableName(ctx, t, mt)
	require.NoError(t, err)
	require.Equal(t, hNew.Name(), mtn2)

	// Write to now-active table
	dest2, err := mt.Put(ctx, "k2", []byte("v2"))
	require.NoError(t, err)
	require.Equal(t, mtn2, dest2)

	// Verify both writes can be read back
	rec1, src1, err := mt.Get(ctx, "k1")
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), rec1.Document)
	require.Equal(t, mtn1, src1)

	rec2, src2, err := mt.Get(ctx, "k2")
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), rec2.Document)
	require.Equal(t, mtn2, src2)

	// Advance clock again before dropping
	c.Advance(1 * time.Second)

	// Drop the old memtable
	err = mt.Drop(ctx, mtn1)
	require.NoError(t, err)

	// Verify k1 is no longer readable
	_, _, err = mt.Get(ctx, "k1")
	require.Error(t, err)
	require.IsType(t, &NotFound{}, err)

	// k2 should still be readable
	rec2, src2, err = mt.Get(ctx, "k2")
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), rec2.Document)
	require.Equal(t, mtn2, src2)
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	// Get the current memtable name
	currentName, err := getCurrentMemtableName(ctx, t, mt)
	require.NoError(t, err)

	// Write to current table
	dest, err := mt.Put(ctx, "k", []byte("vvvv"))
	require.NoError(t, err)
	require.Equal(t, currentName, dest)

	// Check that it made it to mongo
	recs := getRecords(ctx, t, mt, currentName, "k")
	require.Equal(t, []types.Record{
		{
			Key:       "k",
			Timestamp: c.Now().UTC().Truncate(time.Millisecond),
			Document:  []byte("vvvv"),
		},
	}, recs)
}

func TestPutConcurrent(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	// Get the current memtable name
	currentName, err := getCurrentMemtableName(ctx, t, mt)
	require.NoError(t, err)

	// Normal uncontended put
	_, err = mt.Put(ctx, "k", []byte("1111"))
	require.NoError(t, err)
	t1 := c.Now()

	// Now attempt to write to the same key. this will fail, because the time
	// (per the fake clock) is the same as the previous write. it will sleep
	// then retry
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err = mt.Put(ctx, "k", []byte("2222"))
		wg.Done()
	}()

	// Block until Put is sleeping
	ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	require.NoError(t, c.BlockUntilContext(ctx2, 1), "mt.Put did not sleep")

	// Advance 1.2ms to exceed the maximum sleep time (with jitter)
	c.Advance(retrySleep + retryJitter + 1)
	t2 := c.Now()
	wg.Wait()
	require.NoError(t, err)

	// Check that both writes made it to mongo
	recs := getRecords(ctx, t, mt, currentName, "k")
	require.Equal(t, []types.Record{
		{
			Key:       "k",
			Timestamp: t1.UTC().Truncate(time.Millisecond),
			Document:  []byte("1111"),
		},
		{
			Key:       "k",
			Timestamp: t2.UTC().Truncate(time.Millisecond),
			Document:  []byte("2222"),
		},
	}, recs)
}

// Helper function to get the current memtable name
func getCurrentMemtableName(ctx context.Context, t *testing.T, mt *Memtable) (string, error) {
	db, err := mt.GetMongo(ctx)
	require.NoError(t, err)

	res := db.Collection(metaCollection).FindOne(ctx, bson.M{"_id": activeMemtableKey})
	var doc bson.M
	err = res.Decode(&doc)
	if err != nil {
		return "", err
	}

	val, ok := doc["value"]
	if !ok {
		return "", fmt.Errorf("no value key in active memtable doc")
	}

	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("value in active memtable doc was not string")
	}

	return s, nil
}

func getRecords(ctx context.Context, t *testing.T, mt *Memtable, coll string, key string) []types.Record {
	db, err := mt.GetMongo(ctx)
	require.NoError(t, err)

	cur, err := db.Collection(coll).Find(ctx, bson.M{"key": key}, options.Find().SetSort(bson.M{"ts": 1}))
	require.NoError(t, err)
	defer cur.Close(ctx)

	var recs []types.Record
	err = cur.All(ctx, &recs)
	require.NoError(t, err)

	return recs
}
