package txn

import (
	"fmt"
	"os"
	"time"
)

// Error types
type TxnNotFoundError struct {
	TxID string
}

func (e *TxnNotFoundError) Error() string {
	return fmt.Sprintf("transaction not found: %s", e.TxID)
}

type InvalidTxnStatusError struct {
	TxID           string
	CurrentStatus  string
	ExpectedStatus string
}

func (e *InvalidTxnStatusError) Error() string {
	return fmt.Sprintf("transaction %s has invalid status: %s, expected: %s",
		e.TxID, e.CurrentStatus, e.ExpectedStatus)
}

type TxnConflictError struct {
	TxID         string
	ConflictTxID string
	ConflictKey  string
}

func (e *TxnConflictError) Error() string {
	return fmt.Sprintf("transaction %s conflicts with transaction %s on key %s",
		e.TxID, e.ConflictTxID, e.ConflictKey)
}

// generateTxID creates a transaction ID using process ID, counter, and timestamp
// to ensure globally unique IDs even across process restarts
func generateTxID(counter uint64) string {
	return fmt.Sprintf("tx-%d-%d-%d",
		time.Now().UnixNano(),
		os.Getpid(),
		counter)
}

// FormatTxID formats a transaction ID for display purposes
func FormatTxID(txID string) string {
	// Just return the raw ID for now, but we could add formatting in the future
	return txID
}

// IsTxnError checks if an error is a transaction-related error
func IsTxnError(err error) bool {
	switch err.(type) {
	case *TxnNotFoundError, *InvalidTxnStatusError, *TxnConflictError:
		return true
	default:
		return false
	}
}
