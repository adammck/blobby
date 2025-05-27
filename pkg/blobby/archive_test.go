package blobby

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, clock clockwork.Clock) (context.Context, *testdeps.Env, *Blobby) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo(), testdeps.WithMinio())

	sf := sstable.NewFactory(sstable.WithIndexEveryNRecords(8), sstable.WithFilter("mod"))
	b := New(ctx, env.MongoURL(), env.S3Bucket, clock, sf)

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
	require.Equal(t, &api.GetStats{
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
	require.Equal(t, &api.FlushStats{
		FlushedMemtable: t1.memtable,
		ActiveMemtable:  t2.memtable,
		Meta: &api.BlobMeta{
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
	require.Equal(t, &api.GetStats{
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
	require.Equal(t, &api.GetStats{
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
	require.Equal(t, &api.FlushStats{
		FlushedMemtable: t2.memtable,
		ActiveMemtable:  t3.memtable,
		Meta: &api.BlobMeta{
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
	require.Equal(t, &api.GetStats{
		Source:         t2.sstable,
		BlobsFetched:   1,
		RecordsScanned: 2,
	}, gstats)
	val, gstats = tb.get("014")
	require.Equal(t, val, docs["014"])
	require.Equal(t, &api.GetStats{
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
	require.Equal(t, &api.GetStats{
		Source: t3.memtable,
	}, gstats)
	val, gstats = tb.get("013")
	require.Equal(t, val, []byte("yyy"))
	require.Equal(t, &api.GetStats{
		Source: t3.memtable,
	}, gstats)

	// flush again. the two keys we just wrote will end up in the new sstable.
	c.Advance(1 * time.Hour)
	t4 := tb.now()
	fstats, err = b.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, &api.FlushStats{
		FlushedMemtable: t3.memtable,
		ActiveMemtable:  t4.memtable,
		Meta: &api.BlobMeta{
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
	require.Equal(t, &api.GetStats{
		Source:         t4.sstable,
		BlobsFetched:   1, // <--
		RecordsScanned: 1,
	}, gstats)

	// now fetch a key which is in the oldest sstable, and outside of the key
	// range of the sstable we just wrote. we can still do this in one fetch.
	val, gstats = tb.get("002")
	require.Equal(t, val, docs["002"])
	require.Equal(t, &api.GetStats{
		Source:         t2.sstable,
		BlobsFetched:   1, // <--
		RecordsScanned: 2,
	}, gstats)

	// finally, fetch a key which we know was flushed into the middle sstable,
	// but is within the key range of the latest sstable. we need to fetch both
	// sstables, and scan through the first to check that the key isn't present
	// before moving onto the second one.
	//
	// we're using the 'mod' filter, which return false positives for keys with
	// an even suffix (like this one), so we fetch both sstables. later we'll
	// skip some.
	val, gstats = tb.get("012")
	require.Equal(t, val, docs["012"])
	require.Equal(t, &api.GetStats{
		Source:         t3.sstable,
		BlobsFetched:   2, // <--
		BlobsSkipped:   0,
		RecordsScanned: 4,
	}, gstats)

	// -------------------------------------- part three: simple compaction ----

	// perform a full compaction. every sstable merged into one.
	c.Advance(1 * time.Hour)
	t5 := tb.now()
	cstats, err := b.Compact(ctx, api.CompactionOptions{})
	require.NoError(t, err)
	require.Len(t, cstats, 1)
	require.NoError(t, cstats[0].Error)
	require.Equal(t, []*api.BlobMeta{
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
	require.Equal(t, &api.GetStats{
		Source:         t5.sstable,
		BlobsFetched:   1,
		RecordsScanned: 3,
	}, gstats)

	// and another one. same source. we scan six records here, because the
	// sparse index (every eight records; see test setup) allows us to skip
	// straight to record 008.
	val, gstats = tb.get("013")
	require.Equal(t, []byte("yyy"), val)
	require.Equal(t, &api.GetStats{
		Source:         t5.sstable,
		BlobsFetched:   1,
		RecordsScanned: 6,
	}, gstats)

	// check that the old sstables were deleted.
	for _, ts := range []instant{t2, t3, t4} {
		_, err = b.sstm.GetFull(ctx, ts.sstable)
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
	cstats, err = b.Compact(ctx, api.CompactionOptions{
		Order:    api.NewestFirst,
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
	require.Equal(t, &api.BlobMeta{
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
	require.Equal(t, &api.GetStats{
		Source:         t9.sstable,
		BlobsFetched:   1,
		RecordsScanned: 3,
	}, gstats)

	// verify the old uncompacted sstables still exist
	_, err = b.sstm.GetFull(ctx, t5.sstable)
	require.NoError(t, err)
	_, err = b.sstm.GetFull(ctx, t6.sstable)
	require.NoError(t, err)

	// verify the compacted sstables were deleted
	for _, ins := range []instant{t7, t8} {
		_, err = b.sstm.GetFull(ctx, ins.sstable)
		require.Error(t, err)
	}

	// we finish with four sstables:
	//  - [001, 020] at t5 (oldest, full compaction)
	//  - [101, 102] at t6
	//  - [201, 302] at t9
}

func TestDelete(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// put some records first
	c.Advance(15 * time.Millisecond)
	tb.put("key1", []byte("value1"))
	c.Advance(15 * time.Millisecond)
	tb.put("key2", []byte("value2"))
	c.Advance(15 * time.Millisecond)
	tb.put("key3", []byte("value3"))

	// verify they exist
	val, _ := tb.get("key1")
	require.Equal(t, []byte("value1"), val)
	val, _ = tb.get("key2")
	require.Equal(t, []byte("value2"), val)
	val, _ = tb.get("key3")
	require.Equal(t, []byte("value3"), val)

	// delete key2
	c.Advance(15 * time.Millisecond)
	tb.delete("key2")

	// key1 and key3 should still exist
	val, _ = tb.get("key1")
	require.Equal(t, []byte("value1"), val)
	val, _ = tb.get("key3")
	require.Equal(t, []byte("value3"), val)

	// key2 should return NotFound
	val, stats, err := tb.b.Get(tb.ctx, "key2")
	tb.requireNotFound(val, stats, err, "key2")

	// delete non-existent key should work (idempotent)
	c.Advance(15 * time.Millisecond)
	tb.delete("nonexistent")
	val, stats, err = tb.b.Get(tb.ctx, "nonexistent")
	tb.requireNotFound(val, stats, err, "nonexistent")

	// put after delete should work
	c.Advance(15 * time.Millisecond)
	tb.put("key2", []byte("new_value2"))
	val, _ = tb.get("key2")
	require.Equal(t, []byte("new_value2"), val)

	// delete again
	c.Advance(15 * time.Millisecond)
	tb.delete("key2")
	val, stats, err = tb.b.Get(tb.ctx, "key2")
	tb.requireNotFound(val, stats, err, "key2")
}

func TestDeleteWithFlush(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// put and delete in memtable
	c.Advance(15 * time.Millisecond)
	tb.put("key1", []byte("value1"))
	c.Advance(15 * time.Millisecond)
	tb.delete("key1")
	val, stats, err := tb.b.Get(tb.ctx, "key1")
	tb.requireNotFound(val, stats, err, "key1")

	// flush should preserve tombstone
	tb.flush()
	val, stats, err = tb.b.Get(tb.ctx, "key1")
	tb.requireNotFound(val, stats, err, "key1")

	// put new value with same key
	c.Advance(15 * time.Millisecond)
	tb.put("key1", []byte("new_value"))
	val, _ = tb.get("key1")
	require.Equal(t, []byte("new_value"), val)

	// flush again
	tb.flush()
	val, _ = tb.get("key1")
	require.Equal(t, []byte("new_value"), val)
}

func TestDeleteWithCompaction(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// create some data across multiple sstables
	c.Advance(15 * time.Millisecond)
	tb.put("key1", []byte("value1"))
	c.Advance(15 * time.Millisecond)
	tb.put("key2", []byte("value2"))
	tb.flush()

	// advance time and create more data
	c.Advance(1 * time.Hour)
	c.Advance(15 * time.Millisecond)
	tb.put("key3", []byte("value3"))
	c.Advance(15 * time.Millisecond)
	tb.delete("key1") // delete key1
	tb.flush()

	// verify state before compaction
	val, stats, err := tb.b.Get(tb.ctx, "key1")
	tb.requireNotFound(val, stats, err, "key1")
	val, _ = tb.get("key2")
	require.Equal(t, []byte("value2"), val)
	val, _ = tb.get("key3")
	require.Equal(t, []byte("value3"), val)

	// compact all sstables
	c.Advance(1 * time.Hour)
	_, err = b.Compact(ctx, api.CompactionOptions{})
	require.NoError(t, err)

	// verify tombstone is preserved after compaction
	val, stats, err = tb.b.Get(tb.ctx, "key1")
	tb.requireNotFound(val, stats, err, "key1")
	val, _ = tb.get("key2")
	require.Equal(t, []byte("value2"), val)
	val, _ = tb.get("key3")
	require.Equal(t, []byte("value3"), val)
}

func TestDeleteReturnsStatsWithSource(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// put then delete a key
	c.Advance(15 * time.Millisecond)
	memtableName := tb.put("test-key", []byte("test-value"))
	c.Advance(15 * time.Millisecond)
	tb.delete("test-key")

	// get should return notfound with stats showing the memtable source
	_, stats, err := b.Get(ctx, "test-key")
	require.Error(t, err)
	var notFound *api.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "test-key", notFound.Key)
	require.Equal(t, memtableName, stats.Source, "stats should include the memtable where tombstone was found")
}

func TestDeleteReturnsStatsWithSourceFromSSTable(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	tb := &testBlobby{
		ctx: ctx,
		c:   c,
		t:   t,
		b:   b,
	}

	// put and delete a key, then flush to create sstable
	c.Advance(15 * time.Millisecond)
	tb.put("test-key", []byte("test-value"))
	c.Advance(15 * time.Millisecond)
	tb.delete("test-key")
	flushStats := tb.flush()

	// get should return notfound with stats showing the sstable source
	_, stats, err := b.Get(ctx, "test-key")
	require.Error(t, err)
	var notFound *api.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "test-key", notFound.Key)
	require.Equal(t, flushStats.Meta.Filename(), stats.Source, "stats should include the sstable where tombstone was found")
}

type testBlobby struct {
	ctx context.Context
	c   *clockwork.FakeClock
	t   *testing.T
	b   *Blobby
}

func (ta *testBlobby) put(key string, val []byte) string {
	dest, err := ta.b.Put(ta.ctx, key, val)
	require.NoError(ta.t, err)
	return dest
}

func (ta *testBlobby) get(key string) ([]byte, *api.GetStats) {
	val, stats, err := ta.b.Get(ta.ctx, key)
	require.NoError(ta.t, err)
	return val, stats
}

func (ta *testBlobby) delete(key string) *api.DeleteStats {
	stats, err := ta.b.Delete(ta.ctx, key)
	require.NoError(ta.t, err)
	return stats
}

func (ta *testBlobby) requireNotFound(val []byte, stats *api.GetStats, err error, expectedKey string) {
	require.Error(ta.t, err)
	var notFound *api.NotFound
	require.ErrorAs(ta.t, err, &notFound)
	require.Equal(ta.t, expectedKey, notFound.Key)
}

func (ta *testBlobby) flush() *api.FlushStats {
	stats, err := ta.b.Flush(ta.ctx)
	require.NoError(ta.t, err)
	return stats
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
