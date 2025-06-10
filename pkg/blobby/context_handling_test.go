package blobby

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestRangeScanContextPropagation(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create enough data to span multiple sources
	for i := 0; i < 10; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%03d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// flush to sstable
	c.Advance(1 * time.Hour)
	_, err := b.Flush(ctx)
	require.NoError(t, err)

	// add more data in new memtable
	for i := 10; i < 20; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%03d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// create cancellable context
	scanCtx, cancel := context.WithCancel(ctx)

	// start scan
	iter, _, err := b.RangeScan(scanCtx, "", "")
	require.NoError(t, err)
	defer iter.Close()

	// read a few records
	count := 0
	for iter.Next(scanCtx) && count < 5 {
		count++
	}
	require.Equal(t, 5, count)
	require.NoError(t, iter.Err())

	// cancel context
	cancel()

	// next call should detect cancellation
	hasNext := iter.Next(scanCtx)
	if hasNext {
		// some implementations might buffer records, so getting one more is ok
		// but we should eventually hit the cancellation
		hasNext = iter.Next(scanCtx)
		require.False(t, hasNext)
	}

	// should have context cancellation error
	require.Error(t, iter.Err())
	require.Equal(t, context.Canceled, iter.Err())
}

func TestRangeScanContextTimeout(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data
	for i := 0; i < 5; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// create context with very short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	// this should either fail immediately or succeed and then fail on iteration
	iter, _, err := b.RangeScan(timeoutCtx, "", "")
	if err != nil {
		// timeout during scan setup - error might be wrapped
		require.Contains(t, err.Error(), "context deadline exceeded")
		return
	}
	defer iter.Close()

	// if scan creation succeeded, iteration should eventually fail
	for iter.Next(timeoutCtx) {
		// might get some records before timeout
	}

	// should have timeout error - error might be wrapped
	require.Error(t, iter.Err())
	require.Contains(t, iter.Err().Error(), "context deadline exceeded")
}

func TestRangeScanWithDeadlineExceeded(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create data
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Millisecond)
		key := fmt.Sprintf("key-%02d", i)
		_, err := b.Put(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// start scan with good context
	iter, _, err := b.RangeScan(ctx, "", "")
	require.NoError(t, err)
	defer iter.Close()

	// read first record
	require.True(t, iter.Next(ctx))
	require.NoError(t, iter.Err())

	// now use expired context for subsequent reads
	expiredCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-1*time.Hour))
	defer cancel()

	// should fail on next iteration with expired context
	hasNext := iter.Next(expiredCtx)
	if hasNext {
		// if we get another record, the next one should definitely fail
		hasNext = iter.Next(expiredCtx)
		require.False(t, hasNext)
	}

	// should have deadline exceeded error - error might be wrapped
	require.Error(t, iter.Err())
	require.Contains(t, iter.Err().Error(), "context deadline exceeded")
}
