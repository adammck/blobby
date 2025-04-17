package blobby

import (
	"context"
	"fmt"

	"github.com/adammck/blobby/pkg/types"
)

// BeginTransaction starts a new transaction.
func (b *Blobby) BeginTransaction(ctx context.Context) (string, error) {
	return b.txnMgr.Begin(), nil
}

// PutInTransaction adds a key and value to the database within a transaction.
func (b *Blobby) PutInTransaction(ctx context.Context, txID string, key string, value []byte) (string, error) {
	// First add the key to the transaction's write set
	err := b.txnMgr.AddToWriteSet(txID, key)
	if err != nil {
		return "", fmt.Errorf("AddToWriteSet: %w", err)
	}

	// Create the record with the transaction ID
	c, err := b.mt.activeCollection(ctx)
	if err != nil {
		return "", err
	}

	// Insert record with transaction ID
	rec := &types.Record{
		Key:       key,
		Timestamp: b.clock.Now(),
		Document:  value,
		TxID:      txID,
	}

	// Use existing memtable mechanism to insert
	_, err = b.mt.PutRecord(ctx, rec)
	if err != nil {
		return "", err
	}

	return c.Name(), nil
}

// CommitTransaction commits a transaction, making all its writes visible.
func (b *Blobby) CommitTransaction(ctx context.Context, txID string) error {
	return b.txnMgr.Commit(txID)
}

// AbortTransaction aborts a transaction, discarding all its writes.
func (b *Blobby) AbortTransaction(ctx context.Context, txID string) error {
	return b.txnMgr.Abort(txID)
}
