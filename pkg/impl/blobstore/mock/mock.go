package mock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/adammck/blobby/pkg/api"
)

type Store struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

var _ api.BlobStore = (*Store)(nil)

func New() *Store {
	return &Store{
		blobs: make(map[string][]byte),
	}
}

func (m *Store) Put(ctx context.Context, key string, data io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("io.ReadAll: %w", err)
	}

	m.blobs[key] = content
	return nil
}

func (m *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", key)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *Store) GetRange(ctx context.Context, key string, first, last int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", key)
	}

	if first >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	if last == 0 {
		last = int64(len(data)) - 1
	} else if last < first {
		return io.NopCloser(bytes.NewReader(nil)), nil
	} else if last >= int64(len(data)) {
		last = int64(len(data)) - 1
	}

	return io.NopCloser(bytes.NewReader(data[first : last+1])), nil
}

func (m *Store) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.blobs[key]
	if !ok {
		return fmt.Errorf("blob not found: %s", key)
	}

	delete(m.blobs, key)

	return nil
}
