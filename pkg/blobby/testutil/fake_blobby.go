package testutil

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/jonboulle/clockwork"
)

var tombstone []byte = nil

type FakeBlobby struct {
	data  map[string][]byte
	mu    sync.RWMutex
	clock clockwork.Clock
}

var _ api.Blobby = (*FakeBlobby)(nil)

func NewFakeBlobby() *FakeBlobby {
	return &FakeBlobby{
		data:  make(map[string][]byte),
		clock: clockwork.NewRealClock(),
	}
}

func (m *FakeBlobby) Get(ctx context.Context, key string) ([]byte, *api.GetStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.data[key]
	// key doesn't exist or key was deleted (tombstone)
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

	m.data[key] = tombstone
	return &api.DeleteStats{
		Timestamp:   m.clock.Now(),
		Destination: "model",
	}, nil
}

func (m *FakeBlobby) Scan(ctx context.Context, start, end string) (api.Iterator, *api.ScanStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for key := range m.data {
		if m.data[key] == nil { // skip tombstones
			continue
		}
		if start != "" && key < start {
			continue
		}
		if end != "" && key >= end {
			continue
		}
		keys = append(keys, key)
	}

	// sort keys
	sort.Strings(keys)

	return &fakeIterator{
		data: m.data,
		keys: keys,
		pos:  -1,
	}, &api.ScanStats{RecordsReturned: len(keys)}, nil
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

type fakeIterator struct {
	data map[string][]byte
	keys []string
	pos  int
	err  error
}

func (it *fakeIterator) Next(ctx context.Context) bool {
	if it.err != nil || it.pos >= len(it.keys)-1 {
		return false
	}
	it.pos++
	return true
}

func (it *fakeIterator) Key() string {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return ""
	}
	return it.keys[it.pos]
}

func (it *fakeIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	key := it.keys[it.pos]
	return it.data[key]
}

func (it *fakeIterator) Err() error {
	return it.err
}

func (it *fakeIterator) Timestamp() time.Time {
	return time.Now()
}

func (it *fakeIterator) Close() error {
	return nil
}
