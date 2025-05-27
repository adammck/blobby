package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

type FakeBlobby struct {
	data map[string][]byte
	mu   sync.RWMutex
}

var _ api.Blobby = (*FakeBlobby)(nil)

func NewFakeBlobby() *FakeBlobby {
	return &FakeBlobby{
		data: make(map[string][]byte),
	}
}

func (m *FakeBlobby) Get(ctx context.Context, key string) ([]byte, *api.GetStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.data[key]
	if !exists || val == nil {
		return nil, &api.GetStats{}, &api.NotFound{Key: key}
	}
	return val, &api.GetStats{}, nil
}

func (m *FakeBlobby) Put(ctx context.Context, key string, value []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return "model", nil
}

func (m *FakeBlobby) Delete(ctx context.Context, key string) (*api.DeleteStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = nil // nil represents a tombstone
	return &api.DeleteStats{
		Timestamp:   time.Now(),
		Destination: "model",
	}, nil
}

// No-op but returns valid stats structure
func (m *FakeBlobby) Flush(ctx context.Context) (*api.FlushStats, error) {
	return &api.FlushStats{
		ActiveMemtable: "model",
		Meta:           &api.BlobMeta{},
	}, nil
}

// No-op
func (m *FakeBlobby) Compact(ctx context.Context, opts api.CompactionOptions) ([]*api.CompactionStats, error) {
	return []*api.CompactionStats{}, nil
}

func (m *FakeBlobby) Ping(ctx context.Context) error {
	return nil
}

func (m *FakeBlobby) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
	return nil
}
