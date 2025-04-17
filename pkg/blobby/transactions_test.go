package blobby

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestTransactionBasics(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Begin a transaction
	txID, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, txID)

	// Write data in the transaction
	key := "tx_test_key"
	val := []byte("transaction_value")
	dest, err := b.PutInTransaction(ctx, txID, key, val)
	require.NoError(t, err)
	require.NotEmpty(t, dest)

	// Key should not be visible before commit
	_, _, err = b.Get(ctx, key)
	require.Error(t, err)

	// Commit transaction
	err = b.CommitTransaction(ctx, txID)
	require.NoError(t, err)

	// Key should be visible after commit
	result, stats, err := b.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, result)
	require.NotEmpty(t, stats.Source)
}

func TestTransactionAbort(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Begin a transaction
	txID, err := b.BeginTransaction(ctx)
	require.NoError(t, err)

	// Write data in the transaction
	key := "tx_abort_key"
	val := []byte("will_be_aborted")
	_, err = b.PutInTransaction(ctx, txID, key, val)
	require.NoError(t, err)

	// Abort transaction
	err = b.AbortTransaction(ctx, txID)
	require.NoError(t, err)

	// Key should not be visible after abort
	_, _, err = b.Get(ctx, key)
	require.Error(t, err)
}

func TestTransactionConflict(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Begin first transaction
	txID1, err := b.BeginTransaction(ctx)
	require.NoError(t, err)

	// Begin second transaction
	txID2, err := b.BeginTransaction(ctx)
	require.NoError(t, err)

	// Write same key in both transactions
	sharedKey := "conflict_key"
	val1 := []byte("value_from_tx1")
	val2 := []byte("value_from_tx2")

	_, err = b.PutInTransaction(ctx, txID1, sharedKey, val1)
	require.NoError(t, err)

	_, err = b.PutInTransaction(ctx, txID2, sharedKey, val2)
	require.NoError(t, err)

	// Commit first transaction
	err = b.CommitTransaction(ctx, txID1)
	require.NoError(t, err)

	// Second transaction should fail to commit due to conflict
	err = b.CommitTransaction(ctx, txID2)
	require.Error(t, err)

	// Verify the value from the first transaction is visible
	result, _, err := b.Get(ctx, sharedKey)
	require.NoError(t, err)
	require.Equal(t, val1, result)
}

func TestImplicitTransactionVisibility(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Regular put uses implicit transaction
	key := "implicit_tx_key"
	val := []byte("implicit_value")

	_, err := b.Put(ctx, key, val)
	require.NoError(t, err)

	// Value should be immediately visible
	result, _, err := b.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, result)
}

func TestTransactionMemtableFlush(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Begin and commit a transaction
	txID, err := b.BeginTransaction(ctx)
	require.NoError(t, err)

	key := "flush_test_key"
	val := []byte("will_survive_flush")

	_, err = b.PutInTransaction(ctx, txID, key, val)
	require.NoError(t, err)

	err = b.CommitTransaction(ctx, txID)
	require.NoError(t, err)

	// Verify it's visible
	result, _, err := b.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, result)

	// Flush memtable to SSTable
	_, err = b.Flush(ctx)
	require.NoError(t, err)

	// Should still be visible after flush
	result, stats, err := b.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, result)
	require.NotEqual(t, 0, stats.BlobsFetched) // Should come from SSTable now
}

func TestTransactionVisibilityInSSTable(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Write keys in transactions with different states

	// Committed transaction
	txID1, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = b.PutInTransaction(ctx, txID1, "committed_key", []byte("committed_value"))
	require.NoError(t, err)
	err = b.CommitTransaction(ctx, txID1)
	require.NoError(t, err)

	// Aborted transaction
	txID2, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = b.PutInTransaction(ctx, txID2, "aborted_key", []byte("aborted_value"))
	require.NoError(t, err)
	err = b.AbortTransaction(ctx, txID2)
	require.NoError(t, err)

	// Pending transaction
	txID3, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = b.PutInTransaction(ctx, txID3, "pending_key", []byte("pending_value"))
	require.NoError(t, err)

	// Flush to SSTable
	_, err = b.Flush(ctx)
	require.NoError(t, err)

	// Committed key should be visible
	result1, _, err := b.Get(ctx, "committed_key")
	require.NoError(t, err)
	require.Equal(t, []byte("committed_value"), result1)

	// Aborted key should not be visible
	_, _, err = b.Get(ctx, "aborted_key")
	require.Error(t, err)

	// Pending key should not be visible
	_, _, err = b.Get(ctx, "pending_key")
	require.Error(t, err)
}

func TestMultiKeyTransaction(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	c := clockwork.NewFakeClockAt(ts)
	ctx, _, b := setup(t, c)

	// Begin transaction
	txID, err := b.BeginTransaction(ctx)
	require.NoError(t, err)

	// Write multiple keys in same transaction
	_, err = b.PutInTransaction(ctx, txID, "key1", []byte("value1"))
	require.NoError(t, err)
	_, err = b.PutInTransaction(ctx, txID, "key2", []byte("value2"))
	require.NoError(t, err)

	// None should be visible before commit
	_, _, err = b.Get(ctx, "key1")
	require.Error(t, err)
	_, _, err = b.Get(ctx, "key2")
	require.Error(t, err)

	// Commit
	err = b.CommitTransaction(ctx, txID)
	require.NoError(t, err)

	// Both should be visible after commit
	val1, _, err := b.Get(ctx, "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val1)

	val2, _, err := b.Get(ctx, "key2")
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val2)
}
