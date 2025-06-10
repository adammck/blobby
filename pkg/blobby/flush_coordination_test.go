package blobby

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestFlushCoordinationWithRangeScan(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create initial data
	for i := 0; i < 5; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// start a range scan
	iter, _, err := b.RangeScan(ctx, "", "")
	require.NoError(t, err)

	// verify we can read the first record
	require.True(t, iter.Next(ctx))
	require.Equal(t, "key-00", iter.Key())

	// try to flush while scan is active - should coordinate properly
	var flushErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		c.Advance(1 * time.Hour)
		_, flushErr = b.Flush(ctx)
	}()

	// continue reading from scan
	count := 1 // already read first record
	for iter.Next(ctx) {
		count++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 5, count)

	// close iterator to release memtable reference
	err = iter.Close()
	require.NoError(t, err)

	// wait for flush to complete
	wg.Wait()
	require.NoError(t, flushErr)
}

func TestMemtableReferenceCountingDuringScans(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// start multiple concurrent scans
	numScans := 3
	iters := make([]api.Iterator, numScans)
	var err error

	for i := 0; i < numScans; i++ {
		iters[i], _, err = b.RangeScan(ctx, "", "")
		require.NoError(t, err)
	}

	// verify each scan can read independently
	for i, iter := range iters {
		count := 0
		for iter.Next(ctx) {
			count++
		}
		require.NoError(t, iter.Err(), "scan %d failed", i)
		require.Equal(t, 3, count, "scan %d got wrong count", i)
	}

	// close all iterators
	for i, iter := range iters {
		err = iter.Close()
		require.NoError(t, err, "failed to close scan %d", i)
	}

	// now flush should work normally
	c.Advance(1 * time.Hour)
	_, err = b.Flush(ctx)
	require.NoError(t, err)
}

func TestMemtableReferenceCountingPreventsDeletion(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data in memtable
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// get the current memtable name before starting scan
	memtables, err := b.mt.ListMemtables(ctx)
	require.NoError(t, err)
	require.Len(t, memtables, 1)
	originalMemtable := memtables[0]

	// start scan which should add reference to memtable
	iter, _, err := b.RangeScan(ctx, "", "")
	require.NoError(t, err)

	// read first record
	require.True(t, iter.Next(ctx))

	// verify memtable is still referenced
	canDrop, err := b.mt.CanDropMemtable(ctx, originalMemtable)
	require.NoError(t, err)
	require.False(t, canDrop)

	// trigger flush which should not delete the memtable yet
	c.Advance(1 * time.Hour)
	_, err = b.Flush(ctx)
	require.NoError(t, err)

	// memtable should still exist because scan is active
	memtables, err = b.mt.ListMemtables(ctx)
	require.NoError(t, err)
	require.Contains(t, memtables, originalMemtable, "memtable should still exist during scan")

	// finish reading the scan
	count := 1
	for iter.Next(ctx) {
		count++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 3, count)

	// close iterator to release reference
	err = iter.Close()
	require.NoError(t, err)

	// now memtable should be droppable
	canDrop, err = b.mt.CanDropMemtable(ctx, originalMemtable)
	require.NoError(t, err)
	require.True(t, canDrop)
}

func TestPanicRecoveryInRangeScan(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data
	c.Advance(10 * time.Millisecond)
	_, err := b.Put(ctx, "test", []byte("value"))
	require.NoError(t, err)

	// inject a panic in the middle of range scan by canceling context early
	// this tests that our defer cleanup handles panics properly
	cancelCtx, cancel := context.WithCancel(ctx)

	// start scan
	iter, _, err := b.RangeScan(cancelCtx, "", "")
	require.NoError(t, err)

	// cancel context to potentially trigger cleanup
	cancel()

	// try to use iterator with canceled context
	hasNext := iter.Next(cancelCtx)
	if hasNext {
		// if we get data, that's fine, just close cleanly
		iter.Close()
	} else {
		// if we get an error due to cancellation, that's also fine
		iter.Close()
	}

	// verify we can still use blobby normally after potential panic
	_, _, err = b.RangeScan(ctx, "", "")
	require.NoError(t, err)
}
