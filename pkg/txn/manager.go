package txn

import (
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

const (
	StatusPending   = "pending"
	StatusCommitted = "committed"
	StatusAborted   = "aborted"
)

type TxnManager struct {
	mu             sync.RWMutex
	nextTxID       uint64
	transactions   map[string]*Transaction
	clock          clockwork.Clock
	maxTxnLifetime time.Duration
}

type Transaction struct {
	ID         string
	Status     string // "pending", "committed", "aborted"
	StartTs    time.Time
	CommitTs   time.Time
	LastActive time.Time
	WriteSet   map[string]struct{}
	Implicit   bool // true for non-transactional writes
}

func NewTxnManager(clock clockwork.Clock) *TxnManager {
	return &TxnManager{
		transactions:   make(map[string]*Transaction),
		clock:          clock,
		maxTxnLifetime: 10 * time.Minute, // default timeout
	}
}

func (tm *TxnManager) Begin() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := tm.generateTxID()
	now := tm.clock.Now()

	tx := &Transaction{
		ID:         txID,
		Status:     StatusPending,
		StartTs:    now,
		LastActive: now,
		WriteSet:   make(map[string]struct{}),
	}

	tm.transactions[txID] = tx
	return txID
}

func (tm *TxnManager) AddToWriteSet(txID, key string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return &TxnNotFoundError{txID}
	}

	if tx.Status != StatusPending {
		return &InvalidTxnStatusError{txID, tx.Status, StatusPending}
	}

	// Update LastActive timestamp to prevent timeouts
	tx.LastActive = tm.clock.Now()
	tx.WriteSet[key] = struct{}{}

	return nil
}

func (tm *TxnManager) Commit(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return &TxnNotFoundError{txID}
	}

	// Idempotent: already committed is not an error
	if tx.Status == StatusCommitted {
		return nil
	}

	if tx.Status != StatusPending {
		return &InvalidTxnStatusError{txID, tx.Status, StatusPending}
	}

	// Check for conflicts using first-committer-wins semantics
	if err := tm.checkConflicts(tx); err != nil {
		// Mark the transaction as aborted
		tx.Status = StatusAborted
		return err
	}

	// No conflicts, commit the transaction
	tx.Status = StatusCommitted
	tx.CommitTs = tm.clock.Now()
	tx.LastActive = tx.CommitTs

	return nil
}

func (tm *TxnManager) Abort(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return &TxnNotFoundError{txID}
	}

	// Idempotent: already aborted is not an error
	if tx.Status == StatusAborted {
		return nil
	}

	// Can't abort a committed transaction
	if tx.Status == StatusCommitted {
		return &InvalidTxnStatusError{txID, tx.Status, StatusPending}
	}

	tx.Status = StatusAborted
	tx.LastActive = tm.clock.Now()

	return nil
}

func (tm *TxnManager) CreateImplicitTransaction() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := tm.generateTxID()
	now := tm.clock.Now()

	tx := &Transaction{
		ID:         txID,
		Status:     StatusPending,
		StartTs:    now,
		LastActive: now,
		WriteSet:   make(map[string]struct{}),
		Implicit:   true,
	}

	tm.transactions[txID] = tx
	return txID
}

func (tm *TxnManager) GetStatus(txID string) (string, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return "", false
	}

	return tx.Status, true
}

func (tm *TxnManager) PruneOldTransactions() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	threshold := tm.clock.Now().Add(-tm.maxTxnLifetime)

	for txID, tx := range tm.transactions {
		if tx.Status == StatusPending && tx.LastActive.Before(threshold) {
			tx.Status = StatusAborted
			// In a durable implementation, we might eventually clean these up
			// but for now, we'll keep them to maintain proper visibility rules
		}
	}
}

func (tm *TxnManager) checkConflicts(tx *Transaction) error {
	// First-committer-wins semantics:
	// Check for any committed transactions with commit timestamp > our start time
	// that modified any of our keys
	for _, otherTx := range tm.transactions {
		if otherTx.ID == tx.ID {
			continue // Skip ourselves
		}

		if otherTx.Status != StatusCommitted {
			continue // Only check committed transactions
		}

		// Only check transactions that committed after our start time
		if !otherTx.CommitTs.After(tx.StartTs) {
			continue
		}

		// Check for any overlap in the write sets
		for key := range tx.WriteSet {
			if _, exists := otherTx.WriteSet[key]; exists {
				return &TxnConflictError{tx.ID, otherTx.ID, key}
			}
		}
	}

	return nil
}

func (tm *TxnManager) generateTxID() string {
	tm.nextTxID++
	return generateTxID(tm.nextTxID)
}
