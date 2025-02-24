package memtable

import (
	"context"
	"fmt"
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

	// write to default table (blue)
	dest1, err := mt.Put(ctx, "k1", []byte("v1"))
	require.NoError(t, err)
	require.Equal(t, blueMemtableName, dest1)

	// first swap: blue -> green
	hNow, hPrev, err := mt.Swap(ctx)
	require.NoError(t, err)
	require.Equal(t, greenMemtableName, hPrev.Name())

	// write to now-active table (green)
	dest2, err := mt.Put(ctx, "k2", []byte("v2"))
	require.NoError(t, err)
	require.Equal(t, greenMemtableName, dest2)

	// verify both writes can be read back
	rec1, src1, err := mt.Get(ctx, "k1")
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), rec1.Document)
	require.Equal(t, dest1, src1)
	rec2, src2, err := mt.Get(ctx, "k2")
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), rec2.Document)
	require.Equal(t, dest2, src2)

	// try to swap back: green -> blue
	// fails because we haven't truncated blue
	_, _, err = mt.Swap(ctx)
	require.EqualError(t, err, fmt.Sprintf("want to activate %s, but is not empty", blueMemtableName))

	// so truncate it, to drop all the data
	// (note that we didn't flush, we're not testing that here.)
	err = hNow.Truncate(ctx)
	require.NoError(t, err)

	// try to swap back again: green -> blue
	// it works this time
	_, hNow, err = mt.Swap(ctx)
	require.NoError(t, err)
	require.Equal(t, blueMemtableName, hNow.Name())
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	// write to default table (blue)
	dest, err := mt.Put(ctx, "k", []byte("vvvv"))
	require.NoError(t, err)
	require.Equal(t, blueMemtableName, dest)

	// check that it made it to mongo
	recs := getRecords(ctx, t, mt, blueMemtableName, "k")
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

	// normal uncontended put.
	_, err = mt.Put(ctx, "k", []byte("1111"))
	require.NoError(t, err)
	t1 := c.Now()

	// now attempt to write to the same key. this will fail, because the time
	// (per the fake clock) is the same as the previous write. it will sleep
	// then retry
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err = mt.Put(ctx, "k", []byte("2222"))
		wg.Done()
	}()

	// block until Put is sleeping.
	ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	require.NoError(t, c.BlockUntilContext(ctx2, 1), "mt.Put did not sleep")

	// advance 1.2ms to exceed the maximum sleep time (with jitter).
	c.Advance(retrySleep + retryJitter + 1)
	t2 := c.Now()
	wg.Wait()
	require.NoError(t, err)

	// check that both writes made it to mongo.
	recs := getRecords(ctx, t, mt, blueMemtableName, "k")
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
