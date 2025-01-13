package archive

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *testutil.Env, *Archive, *clockwork.FakeClock) {
	ctx := context.Background()
	env := testutil.SetupTest(ctx, t)

	clock := clockwork.NewFakeClock()
	arc := New(env.MongoURL, env.S3Bucket, clock)

	err := arc.Init(ctx)
	require.NoError(t, err)

	err = arc.Ping(ctx)
	require.NoError(t, err)

	return ctx, env, arc, clock
}

func TestBasicWriteRead(t *testing.T) {
	ctx, env, a, c := setup(t)

	// wrap archive in test helper to make this readable
	ta := &testArchive{
		ctx: ctx,
		t:   t,
		a:   a,
	}

	// prepare n docs full of junk
	docs := map[string][]byte{}
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("%03d", i)
		docs[k] = []byte(strings.Repeat(k, 3))
	}

	for k, v := range docs {
		ta.put(k, v)
	}

	// fetch an arbitrary key. they're all sitting in the default memtable
	// because we haven't flushed anything.
	val, gstats := ta.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("%s/archive/blue", env.MongoURL),
		BlobsFetched:   0,
		RecordsScanned: 0,
	}, gstats)

	// and another one
	val, _ = ta.get("005")
	require.Equal(t, val, docs["005"])

	// flush memtable to the blobstore
	f1t := c.Now()
	fstats, err := a.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: "",
		ActiveMemtable:  fmt.Sprintf("%s/archive/green", env.MongoURL),
		BlobURL:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f1t.Unix()),
		Meta: &sstable.Meta{
			MinKey:  "001",
			MaxKey:  "010",
			Count:   10,
			Size:    497, // idk lol
			Created: f1t,
		},
	}, fstats)

	// fetch the same key, and see that it's now read from the blobstore.
	val, gstats = ta.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f1t.Unix()),
		BlobsFetched:   1,
		RecordsScanned: 1,
	}, gstats)

	// fetch the other one to show how inefficient our linear scan is. yikes.
	val, gstats = ta.get("005")
	require.Equal(t, val, docs["005"])
	require.Equal(t, gstats.RecordsScanned, 5)

	// write ten more new documents. they'll end up in the other memtable.
	for i := 11; i <= 20; i++ {
		k := fmt.Sprintf("%03d", i)
		docs[k] = []byte(strings.Repeat(k, 3))
		ta.put(k, docs[k])
	}

	// fetch one of the new keys. it's in the other memtable.
	val, gstats = ta.get("015")
	require.Equal(t, val, docs["015"])
	require.Equal(t, &GetStats{
		Source: fmt.Sprintf("%s/archive/green", env.MongoURL),
	}, gstats)

	// pass some time, so the second sstable will have a different URL. (they're
	// just named by the current time for now.)
	c.Advance(1 * time.Second)

	// flush again. note that the keys in this sstable are totally disjoint from
	// the first.
	f2t := c.Now()
	fstats, err = a.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: "", // TODO
		ActiveMemtable:  fmt.Sprintf("%s/archive/blue", env.MongoURL),
		BlobURL:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f2t.Unix()),
		Meta: &sstable.Meta{
			MinKey:  "011",
			MaxKey:  "020",
			Count:   10,
			Size:    497,
			Created: f2t,
		},
	}, fstats)

	// fetch two keys, to show that they're in the different sstables, but that
	// we only needed to fetch one of them for each get.
	val, gstats = ta.get("002")
	require.Equal(t, val, docs["002"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f1t.Unix()),
		BlobsFetched:   1,
		RecordsScanned: 2,
	}, gstats)
	val, gstats = ta.get("014")
	require.Equal(t, val, docs["014"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f2t.Unix()),
		BlobsFetched:   1,
		RecordsScanned: 4,
	}, gstats)

	// write new versions of two of the keys to the memtable. note that both of
	// them already exist different sstables.
	// TODO: PutStats
	ta.put("003", []byte("xxx"))
	ta.put("013", []byte("yyy"))

	// fetch them to show that we're reading from the memtable, and getting the
	// new values back. the values in the sstables are masked.
	val, gstats = ta.get("003")
	require.Equal(t, val, []byte("xxx"))
	require.Equal(t, &GetStats{
		Source: fmt.Sprintf("%s/archive/blue", env.MongoURL),
	}, gstats)
	val, gstats = ta.get("013")
	require.Equal(t, val, []byte("yyy"))
	require.Equal(t, &GetStats{
		Source: fmt.Sprintf("%s/archive/blue", env.MongoURL),
	}, gstats)

	// flush again. the two keys we just wrote will end up in the new sstable.
	c.Advance(1 * time.Second)
	f3t := c.Now()
	fstats, err = a.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: "", // TODO
		ActiveMemtable:  fmt.Sprintf("%s/archive/green", env.MongoURL),
		BlobURL:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f3t.Unix()),
		Meta: &sstable.Meta{
			MinKey:  "003",
			MaxKey:  "013",
			Count:   2,
			Size:    93,
			Created: f3t,
		},
	}, fstats)

	// now we have three sstables with the key ranges:
	//  - [001, 010]
	//  - [011, 020]
	//  - [003, 013]

	// fetch a key which we know is in the newest sstable. note that we only
	// need to fetch one sstable, because we start at the newest one, and that
	// we only need to scan through a single record, since not all keys are
	// present.
	val, gstats = ta.get("003")
	require.Equal(t, val, []byte("xxx"))
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f3t.Unix()),
		BlobsFetched:   1, // <--
		RecordsScanned: 1,
	}, gstats)

	// now fetch a key which is in the oldest sstable, and outside of the key
	// range of the sstable we just wrote. we can still do this in one fetch.
	val, gstats = ta.get("002")
	require.Equal(t, val, docs["002"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f1t.Unix()),
		BlobsFetched:   1, // <--
		RecordsScanned: 2,
	}, gstats)

	// finally, fetch a key which we know was flushed into the middle sstable,
	// but is within the key range of the latest sstable. we need to fetch both
	// sstables, and scan through the first to check that the key isn't present
	// before moving onto the second one.
	//
	// later, we'll optimize this with bloom filters, so we can often skip the
	// first fetch. not implemented yet. we'll also index them, so we can fetch
	// a subset of keys, but that's also not implemented.
	val, gstats = ta.get("012")
	require.Equal(t, val, docs["012"])
	require.Equal(t, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, f2t.Unix()),
		BlobsFetched:   2, // <--
		RecordsScanned: 4, // (003, 013), (011, 012)
	}, gstats)
}

type testArchive struct {
	ctx context.Context
	t   *testing.T
	a   *Archive
}

func (ta *testArchive) put(key string, val []byte) string {
	dest, err := ta.a.Put(ta.ctx, key, val)
	require.NoError(ta.t, err)
	return dest
}

func (ta *testArchive) get(key string) ([]byte, *GetStats) {
	val, stats, err := ta.a.Get(ta.ctx, key)
	require.NoError(ta.t, err)
	return val, stats
}
