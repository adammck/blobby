package mock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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

	content, err := ioutil.ReadAll(data)
	if err != nil {
		return fmt.Errorf("ReadAll: %w", err)
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

	// treat zero end as EOF
	if last == 0 {
		last = int64(len(data)) - 1
	}
	if last < first {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if last >= int64(len(data)) {
		last = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[first : last+1])), nil
}

func (m *Store) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.blobs[key]; !ok {
		return fmt.Errorf("blob not found: %s", key)
	}

	delete(m.blobs, key)
	return nil
}
