package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/adammck/blobby/pkg/api"
)

type TxHarness struct {
	*Harness
	activeTxs map[string]bool
}

func NewTxHarness(h *Harness) *TxHarness {
	return &TxHarness{
		Harness:   h,
		activeTxs: make(map[string]bool),
	}
}

func (th *TxHarness) BeginTx() Op {
	return BeginTxOp{h: th}
}

func (th *TxHarness) PutInTx(txID string, key string, value []byte) Op {
	return PutInTxOp{h: th, txID: txID, k: key, v: value}
}

func (th *TxHarness) CommitTx(txID string) Op {
	return CommitTxOp{h: th, txID: txID}
}

func (th *TxHarness) AbortTx(txID string) Op {
	return AbortTxOp{h: th, txID: txID}
}

func (th *TxHarness) HasActiveTx() bool {
	return len(th.activeTxs) > 0
}

func (th *TxHarness) GetActiveTxIDs() []string {
	txIDs := make([]string, 0, len(th.activeTxs))
	for txID := range th.activeTxs {
		txIDs = append(txIDs, txID)
	}
	return txIDs
}

func (th *TxHarness) NumActiveTx() int {
	return len(th.activeTxs)
}

type BeginTxOp struct {
	h *TxHarness
}

func (o BeginTxOp) Run(t *testing.T, ctx context.Context) error {
	txID, err := o.h.sut.(TransactionalBlobby).BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("BeginTransaction: %w", err)
	}
	o.h.activeTxs[txID] = true
	return nil
}

func (o BeginTxOp) String() string {
	return "BeginTx"
}

type PutInTxOp struct {
	h    *TxHarness
	txID string
	k    string
	v    []byte
}

func (o PutInTxOp) Run(t *testing.T, ctx context.Context) error {
	if _, active := o.h.activeTxs[o.txID]; !active {
		return fmt.Errorf("transaction %s is not active", o.txID)
	}

	_, err := o.h.sut.(TransactionalBlobby).PutInTransaction(ctx, o.txID, o.k, o.v)
	if err != nil {
		return fmt.Errorf("PutInTransaction: %w", err)
	}

	// Track key for verification (but only add to model when committed)
	o.h.keys[o.k] = struct{}{}

	return nil
}

func (o PutInTxOp) String() string {
	return fmt.Sprintf("PutInTx(%s, %s, %s)", o.txID, o.k, o.v)
}

type CommitTxOp struct {
	h    *TxHarness
	txID string
}

func (o CommitTxOp) Run(t *testing.T, ctx context.Context) error {
	if _, active := o.h.activeTxs[o.txID]; !active {
		return fmt.Errorf("transaction %s is not active", o.txID)
	}

	err := o.h.sut.(TransactionalBlobby).CommitTransaction(ctx, o.txID)
	delete(o.h.activeTxs, o.txID)

	// Only make stats changes for successful commits
	if err == nil {
		o.h.stats.TxCommit++
	}

	return err
}

func (o CommitTxOp) String() string {
	return fmt.Sprintf("CommitTx(%s)", o.txID)
}

type AbortTxOp struct {
	h    *TxHarness
	txID string
}

func (o AbortTxOp) Run(t *testing.T, ctx context.Context) error {
	if _, active := o.h.activeTxs[o.txID]; !active {
		return fmt.Errorf("transaction %s is not active", o.txID)
	}

	err := o.h.sut.(TransactionalBlobby).AbortTransaction(ctx, o.txID)
	delete(o.h.activeTxs, o.txID)

	if err == nil {
		o.h.stats.TxAbort++
	}

	return err
}

func (o AbortTxOp) String() string {
	return fmt.Sprintf("AbortTx(%s)", o.txID)
}

type TransactionalBlobby interface {
	api.Blobby
	BeginTransaction(ctx context.Context) (string, error)
	PutInTransaction(ctx context.Context, txID string, key string, value []byte) (string, error)
	CommitTransaction(ctx context.Context, txID string) error
	AbortTransaction(ctx context.Context, txID string) error
}
