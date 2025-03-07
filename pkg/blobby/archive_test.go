package blobby

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/compactor"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, clock clockwork.Clock) (context.Context, *testdeps.Env, *Blobby) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo(), testdeps.WithMinio())

	b := New(env.MongoURL(), env.S3Bucket, clock)

	err := b.Init(ctx)
	require.NoError(t, err)

	err = b.Ping(ctx)
	require.NoError(t, err)

	return ctx, env, b
}

func TestBasicWriteRead(t *testing.T) {

	// Fix the clock to the current time, but simplify things by rounding to the
	// previous second. BSON encoding only supports milliseconds, so we lose the
	// nanoseconds when we round-trip through BSON, making comparisons annoying.
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// wrap blobby in test helper to make this readable
	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// -------------------------------------- part one: inserts and flushes ----

	t1 := tb.now()

	// prepare n docs full of junk and write them all to the memtable. note that
	// there's no overwriting, because each one has a unique key.
	docs := map[string][]byte{}
	for i := 1; i <= 10; i++ {

		// Offset each write by 10ms, so each has a different but predictable
		// timestamp. This is annoying, but important to validate ordering.
		c.Advance(15 * time.Millisecond)

		k := fmt.Sprintf("%03d", i)
		docs[k] = []byte(strings.Repeat(k, 3))
		tb.put(k, docs[k])
	}

	// fetch an arbitrary key. they're all sitting in the default memtable
	// because we haven't flushed anything.
	val, gstats := tb.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, &GetStats{
		Source:         t1.memtable,
		BlobsFetched:   0,
		RecordsScanned: 0,
	}, gstats)

	// and another one
	val, _ = tb.get("005")
	require.Equal(t, val, docs["005"])

	// flush memtable to the blobstore
	t2 := tb.now()
	fstats, err := b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: t1.memtable,
		ActiveMemtable:  t2.memtable,
		BlobName:        t2.sstable,
		Meta: &sstable.Meta{
			MinKey:  "001",
			MaxKey:  "010",
			MinTime: t1.t.Add(15 * time.Millisecond),
			MaxTime: t1.t.Add(15 * time.Millisecond * 10),
			Count:   10,
			Size:    496, // unverified (didn't check, just pasted it)
			Created: t2.t,
		},
	}, fstats)

	// fetch the same key, and see that it's now read from the blobstore.
	val, gstats = tb.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, &GetStats{
		Source:         t2.sstable,
		BlobsFetched:   1,
		RecordsScanned: 1,
	}, gstats)

	// fetch the other one to show how inefficient our linear scan is. yikes.
	val, gstats = tb.get("005")
	require.Equal(t, val, docs["005"])
	require.Equal(t, gstats.RecordsScanned, 5)

	// write ten more new documents. they'll end up in the new memtable.
	for i := 11; i <= 20; i++ {
		// see explanation above.
		c.Advance(15 * time.Millisecond)

		k := fmt.Sprintf("%03d", i)
		docs[k] = []byte(strings.Repeat(k, 3))
		tb.put(k, docs[k])
	}

	// fetch one of the new keys. it's in the second memtable.
	val, gstats = tb.get("015")
	require.Equal(t, val, docs["015"])
	require.Equal(t, &GetStats{
		Source: t2.memtable,
	}, gstats)

	// pass some time, so the second sstable will have a different URL. (they're
	// just named by the current time for now.)
	c.Advance(1 * time.Hour)

	// flush again. note that the keys in this sstable are totally disjoint from
	// the first.
	t3 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: t2.memtable,
		ActiveMemtable:  t3.memtable,
		BlobName:        t3.sstable,
		Meta: &sstable.Meta{
			MinKey:  "011",
			MaxKey:  "020",
			MinTime: t2.t.Add(15 * time.Millisecond),
			MaxTime: t2.t.Add(15 * time.Millisecond * 10),
			Count:   10,
			Size:    496, // unverified
			Created: t3.t,
		},
	}, fstats)

	// fetch two keys, to show that they're in the different sstables, but that
	// we only needed to fetch one of them for each get.
	val, gstats = tb.get("002")
	require.Equal(t, val, docs["002"])
	require.Equal(t, &GetStats{
		Source:         t2.sstable,
		BlobsFetched:   1,
		RecordsScanned: 2,
	}, gstats)
	val, gstats = tb.get("014")
	require.Equal(t, val, docs["014"])
	require.Equal(t, &GetStats{
		Source:         t3.sstable,
		BlobsFetched:   1,
		RecordsScanned: 4,
	}, gstats)

	// ------------------------- part two: updates, or masking old versions ----

	// write new versions of two of the keys to the memtable. note that both of
	// them already exist different sstables.
	// TODO: PutStats

	c.Advance(15 * time.Millisecond)
	tb.put("003", []byte("xxx"))

	c.Advance(15 * time.Millisecond)
	tb.put("013", []byte("yyy"))

	// fetch them to show that we're reading from the memtable, and getting the
	// new values back. the values in the sstables are masked.
	val, gstats = tb.get("003")
	require.Equal(t, val, []byte("xxx"))
	require.Equal(t, &GetStats{
		Source: t3.memtable,
	}, gstats)
	val, gstats = tb.get("013")
	require.Equal(t, val, []byte("yyy"))
	require.Equal(t, &GetStats{
		Source: t3.memtable,
	}, gstats)

	// flush again. the two keys we just wrote will end up in the new sstable.
	c.Advance(1 * time.Hour)
	t4 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &FlushStats{
		FlushedMemtable: t3.memtable,
		ActiveMemtable:  t4.memtable,
		BlobName:        t4.sstable,
		Meta: &sstable.Meta{
			MinKey:  "003",
			MaxKey:  "013",
			MinTime: t3.t.Add(15 * time.Millisecond),
			MaxTime: t3.t.Add(15 * time.Millisecond * 2),
			Count:   2,
			Size:    92,
			Created: t4.t,
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
	val, gstats = tb.get("003")
	require.Equal(t, val, []byte("xxx"))
	require.Equal(t, &GetStats{
		Source:         t4.sstable,
		BlobsFetched:   1, // <--
		RecordsScanned: 1,
	}, gstats)

	// now fetch a key which is in the oldest sstable, and outside of the key
	// range of the sstable we just wrote. we can still do this in one fetch.
	val, gstats = tb.get("002")
	require.Equal(t, val, docs["002"])
	require.Equal(t, &GetStats{
		Source:         t2.sstable,
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
	val, gstats = tb.get("012")
	require.Equal(t, val, docs["012"])
	require.Equal(t, &GetStats{
		Source:         t3.sstable,
		BlobsFetched:   2, // <--
		RecordsScanned: 4, // (003, 013), (011, 012)
	}, gstats)

	// -------------------------------------- part three: simple compaction ----

	// perform a full compaction. every sstable merged into one.
	c.Advance(1 * time.Hour)
	t5 := tb.now()
	cstats, err := b.Compact(ctx, CompactionOptions{})
	require.NoError(t, err)
	require.Len(t, cstats, 1)
	require.NoError(t, cstats[0].Error)
	require.Equal(t, []*sstable.Meta{
		{
			MinKey:  "001",
			MaxKey:  "020",
			MinTime: t1.t.Add(15 * time.Millisecond),
			MaxTime: t3.t.Add(15 * time.Millisecond * 2),
			Count:   22,
			Size:    1072,
			Created: t5.t,
		},
	}, cstats[0].Outputs)

	// not asserting the inputs. too long. trust me, bro.
	require.Len(t, cstats[0].Inputs, 3)

	// now we have one sstable, with the entire key range:
	//  - [001, 020]

	// read one of our previously-read keys. note that it is read out of the new
	// blob, which was output by the compaction, and that only a single blob was
	// fetched and scanned.
	val, gstats = tb.get("003")
	require.Equal(t, []byte("xxx"), val)
	require.Equal(t, &GetStats{
		Source:         t5.sstable,
		BlobsFetched:   1,
		RecordsScanned: 3,
	}, gstats)

	// and another one. same source.
	val, gstats = tb.get("013")
	require.Equal(t, []byte("yyy"), val)
	require.Equal(t, &GetStats{
		Source:         t5.sstable,
		BlobsFetched:   1,
		RecordsScanned: 14,
	}, gstats)

	// check that the old sstables were deleted.
	for _, ts := range []instant{t2, t3, t4} {
		_, _, err = b.bs.Find(ctx, ts.sstable, "001")
		require.Error(t, err)
	}

	// ------------------------------------- part four: flexible compaction ----

	// write some new records that won't overlap with any other keys
	c.Advance(15 * time.Millisecond)
	tb.put("101", []byte("a1"))
	c.Advance(15 * time.Millisecond)
	tb.put("102", []byte("a2"))

	// flush to create second sstable
	c.Advance(1 * time.Hour)
	t6 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, fstats.Meta.Count)

	// write more records
	c.Advance(15 * time.Millisecond)
	tb.put("201", []byte("b1"))
	c.Advance(15 * time.Millisecond)
	tb.put("202", []byte("b2"))

	// flush to create third sstable
	c.Advance(1 * time.Hour)
	t7 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, fstats.Meta.Count)

	// write final records
	c.Advance(15 * time.Millisecond)
	tb.put("301", []byte("c1"))
	c.Advance(15 * time.Millisecond)
	tb.put("302", []byte("c2"))

	// flush to create fourth sstable
	c.Advance(1 * time.Hour)
	t8 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, fstats.Meta.Count)

	// now we have four sstables:
	//  - [001, 020] at t5 (oldest, full compaction from before)
	//  - [101, 102] at t6
	//  - [201, 202] at t7
	//  - [301, 302] at t8 (newest)

	// compact only the two newest files together
	c.Advance(1 * time.Hour)
	t9 := tb.now()
	cstats, err = b.Compact(ctx, CompactionOptions{
		Order:    compactor.NewestFirst,
		MaxFiles: 2,
	})
	require.NoError(t, err)
	require.Len(t, cstats, 1)
	require.NoError(t, cstats[0].Error)

	// verify only newest two files were inputs
	require.Len(t, cstats[0].Inputs, 2)
	require.Equal(t, t8.t.Unix(), cstats[0].Inputs[0].Created.Unix())
	require.Equal(t, t7.t.Unix(), cstats[0].Inputs[1].Created.Unix())

	// verify output metadata
	require.Len(t, cstats[0].Outputs, 1)
	require.Equal(t, &sstable.Meta{
		MinKey:  "201",
		MaxKey:  "302",
		MinTime: t6.t.Add(15 * time.Millisecond * 1),
		MaxTime: t7.t.Add(15 * time.Millisecond * 2),
		Count:   4,
		Size:    174,
		Created: t9.t,
	}, cstats[0].Outputs[0])

	// verify we can read from the newly compacted file
	val, gstats = tb.get("301")
	require.Equal(t, []byte("c1"), val)
	require.Equal(t, &GetStats{
		Source:         t9.sstable,
		BlobsFetched:   1,
		RecordsScanned: 3,
	}, gstats)

	// verify the old uncompacted sstables still exist
	_, _, err = b.bs.Find(ctx, t5.sstable, "001")
	require.NoError(t, err)
	_, _, err = b.bs.Find(ctx, t6.sstable, "101")
	require.NoError(t, err)

	// verify the compacted sstables were deleted
	for _, ins := range []instant{t7, t8} {
		_, _, err = b.bs.Find(ctx, ins.sstable, "001")
		require.Error(t, err)
	}

	// we finish with four sstables:
	//  - [001, 020] at t5 (oldest, full compaction)
	//  - [101, 102] at t6
	//  - [201, 302] at t9
}

type testBlobby struct {
	ctx context.Context
	c   clockwork.Clock
	t   *testing.T
	b   *Blobby
}

func (ta *testBlobby) put(key string, val []byte) string {
	dest, err := ta.b.Put(ta.ctx, key, val)
	require.NoError(ta.t, err)
	return dest
}

func (ta *testBlobby) get(key string) ([]byte, *GetStats) {
	val, stats, err := ta.b.Get(ta.ctx, key)
	require.NoError(ta.t, err)
	return val, stats
}

type instant struct {
	t        time.Time
	memtable string
	sstable  string
}

// now returns the current (fake) time, in a handy struct which also contains
// the name of the memtable and sstable which would be created at that time.
// this is just to avoid baking the filename patterns into the tests.
func (ta *testBlobby) now() instant {
	t := ta.c.Now()

	return instant{
		t:        t,
		memtable: fmt.Sprintf("mt_%d", t.UTC().UnixNano()),
		sstable:  fmt.Sprintf("%d.sstable", t.UnixMilli()),
	}
}
