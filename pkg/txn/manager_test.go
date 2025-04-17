package txn

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestBasicTransactionOperations(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Begin a transaction
	txID := tm.Begin()
	require.NotEmpty(t, txID)

	// Check initial status
	status, exists := tm.GetStatus(txID)
	require.True(t, exists)
	require.Equal(t, StatusPending, status)

	// Add to write set
	err := tm.AddToWriteSet(txID, "testkey")
	require.NoError(t, err)

	// Commit transaction
	err = tm.Commit(txID)
	require.NoError(t, err)

	// Check committed status
	status, exists = tm.GetStatus(txID)
	require.True(t, exists)
	require.Equal(t, StatusCommitted, status)
}

func TestAbortTransaction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Begin a transaction
	txID := tm.Begin()

	// Add to write set
	err := tm.AddToWriteSet(txID, "testkey")
	require.NoError(t, err)

	// Abort transaction
	err = tm.Abort(txID)
	require.NoError(t, err)

	// Check aborted status
	status, exists := tm.GetStatus(txID)
	require.True(t, exists)
	require.Equal(t, StatusAborted, status)
}

func TestCommitIdempotency(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	txID := tm.Begin()

	// First commit should succeed
	err := tm.Commit(txID)
	require.NoError(t, err)

	// Second commit should be idempotent
	err = tm.Commit(txID)
	require.NoError(t, err)
}

func TestAbortIdempotency(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	txID := tm.Begin()

	// First abort should succeed
	err := tm.Abort(txID)
	require.NoError(t, err)

	// Second abort should be idempotent
	err = tm.Abort(txID)
	require.NoError(t, err)
}

func TestInvalidTransactionOperations(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	txID := tm.Begin()

	// Commit the transaction
	err := tm.Commit(txID)
	require.NoError(t, err)

	// Attempt to abort a committed transaction should fail
	err = tm.Abort(txID)
	require.Error(t, err)
	require.IsType(t, &InvalidTxnStatusError{}, err)

	// Begin a new transaction
	txID = tm.Begin()

	// Abort the transaction
	err = tm.Abort(txID)
	require.NoError(t, err)

	// Attempt to add to write set of aborted transaction should fail
	err = tm.AddToWriteSet(txID, "testkey")
	require.Error(t, err)
	require.IsType(t, &InvalidTxnStatusError{}, err)
}

func TestTransactionConflictDetection(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Begin first transaction
	txID1 := tm.Begin()
	err := tm.AddToWriteSet(txID1, "sharedkey")
	require.NoError(t, err)

	// Begin second transaction
	clock.Advance(time.Second)
	txID2 := tm.Begin()
	err = tm.AddToWriteSet(txID2, "sharedkey")
	require.NoError(t, err)

	// Commit first transaction
	err = tm.Commit(txID1)
	require.NoError(t, err)

	// Commit second transaction should fail with conflict
	err = tm.Commit(txID2)
	require.Error(t, err)
	require.IsType(t, &TxnConflictError{}, err)
}

func TestImplicitTransaction(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Create implicit transaction
	txID := tm.CreateImplicitTransaction()
	require.NotEmpty(t, txID)

	// Add to write set
	err := tm.AddToWriteSet(txID, "testkey")
	require.NoError(t, err)

	// Commit should succeed
	err = tm.Commit(txID)
	require.NoError(t, err)

	// Verify it's marked as implicit
	tx, exists := tm.transactions[txID]
	require.True(t, exists)
	require.True(t, tx.Implicit)
}

func TestPruneOldTransactions(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)
	tm.maxTxnLifetime = 10 * time.Minute

	// Create a transaction
	txID := tm.Begin()

	// Advance clock beyond the transaction timeout
	clock.Advance(11 * time.Minute)

	// Prune should mark the transaction as aborted
	tm.PruneOldTransactions()

	// Check status
	status, exists := tm.GetStatus(txID)
	require.True(t, exists)
	require.Equal(t, StatusAborted, status)
}

func TestNonConflictingTransactions(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Begin first transaction
	txID1 := tm.Begin()
	err := tm.AddToWriteSet(txID1, "key1")
	require.NoError(t, err)

	// Begin second transaction
	txID2 := tm.Begin()
	err = tm.AddToWriteSet(txID2, "key2")
	require.NoError(t, err)

	// Commit both transactions should succeed
	err = tm.Commit(txID1)
	require.NoError(t, err)

	err = tm.Commit(txID2)
	require.NoError(t, err)
}

func TestTransactionNotFound(t *testing.T) {
	clock := clockwork.NewFakeClock()
	tm := NewTxnManager(clock)

	// Operations on non-existent transaction should fail
	err := tm.AddToWriteSet("nonexistent", "testkey")
	require.Error(t, err)
	require.IsType(t, &TxnNotFoundError{}, err)

	err = tm.Commit("nonexistent")
	require.Error(t, err)
	require.IsType(t, &TxnNotFoundError{}, err)

	err = tm.Abort("nonexistent")
	require.Error(t, err)
	require.IsType(t, &TxnNotFoundError{}, err)
}
