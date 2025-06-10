package blobby

import (
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestRangeScanStatsTracking(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data across multiple sources
	for i := 0; i < 5; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// flush to create sstable
	c.Advance(1 * time.Hour)
	_, err := b.Flush(ctx)
	require.NoError(t, err)

	// add more data to new memtable
	for i := 5; i < 10; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// perform range scan
	iter, stats, err := b.RangeScan(ctx, "", "")
	require.NoError(t, err)
	defer iter.Close()

	// consume all records
	count := 0
	for iter.Next(ctx) {
		count++
	}
	require.NoError(t, iter.Err())

	// verify stats
	require.Equal(t, 10, count)
	require.Equal(t, 10, stats.RecordsReturned)
	require.Equal(t, 1, stats.MemtablesRead)  // active memtable
	require.Equal(t, 1, stats.SstablesRead)   // one sstable created from flush
	require.Equal(t, 1, stats.BlobsFetched)   // one sstable blob fetched
	require.Equal(t, 0, stats.BlobsSkipped)   // no blooms to skip
	require.Equal(t, 0, stats.RecordsScanned) // this stat is for point gets, not scans
}

func TestRangeScanStatsWithMultipleSources(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create first batch
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("batch1-%02d", i)
		_, err := b.Put(ctx, key, []byte("value1"))
		require.NoError(t, err)
	}

	// flush to first sstable
	c.Advance(1 * time.Hour)
	_, err := b.Flush(ctx)
	require.NoError(t, err)

	// create second batch
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("batch2-%02d", i)
		_, err := b.Put(ctx, key, []byte("value2"))
		require.NoError(t, err)
	}

	// flush to second sstable
	c.Advance(1 * time.Hour)
	_, err = b.Flush(ctx)
	require.NoError(t, err)

	// create third batch in active memtable
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("batch3-%02d", i)
		_, err := b.Put(ctx, key, []byte("value3"))
		require.NoError(t, err)
	}

	// perform range scan
	iter, stats, err := b.RangeScan(ctx, "", "")
	require.NoError(t, err)
	defer iter.Close()

	// consume all records
	count := 0
	for iter.Next(ctx) {
		count++
	}
	require.NoError(t, iter.Err())

	// verify stats
	require.Equal(t, 9, count)
	require.Equal(t, 9, stats.RecordsReturned)
	require.Equal(t, 1, stats.MemtablesRead) // active memtable
	require.Equal(t, 2, stats.SstablesRead)  // two sstables
	require.Equal(t, 2, stats.BlobsFetched)  // two sstable blobs fetched
	require.Equal(t, 0, stats.BlobsSkipped)
	require.Equal(t, 0, stats.RecordsScanned)
}

func TestRangeScanStatsWithLimitedRange(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		c.Advance(10 * time.Millisecond)
		_, err := b.Put(ctx, key, []byte("fruit"))
		require.NoError(t, err)
	}

	// flush some data
	c.Advance(1 * time.Hour)
	_, err := b.Flush(ctx)
	require.NoError(t, err)

	// scan limited range
	iter, stats, err := b.RangeScan(ctx, "banana", "date")
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	for iter.Next(ctx) {
		count++
	}
	require.NoError(t, iter.Err())

	// should get banana and cherry (date is exclusive)
	require.Equal(t, 2, count)
	require.Equal(t, 2, stats.RecordsReturned)
	require.Greater(t, stats.MemtablesRead, 0)
	require.Greater(t, stats.SstablesRead, 0)
}
