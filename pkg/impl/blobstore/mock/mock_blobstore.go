// pkg/impl/blobstore/memory/memory.go
package mock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
)

type BlobStore struct {
	blobs   map[string][]byte
	mu      sync.RWMutex
	clock   clockwork.Clock
	factory sstable.Factory
}

var _ api.BlobStore = (*BlobStore)(nil)

func New(clock clockwork.Clock, factory sstable.Factory) *BlobStore {
	return &BlobStore{
		blobs:   make(map[string][]byte),
		clock:   clock,
		factory: factory,
	}
}

func (m *BlobStore) GetFull(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", key)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *BlobStore) GetPartial(ctx context.Context, key string, first, last int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", key)
	}

	if first >= int64(len(data)) {
		return nil, fmt.Errorf("range start exceeds blob size")
	}

	endPos := int64(len(data))
	if last > 0 && last < endPos {
		endPos = last + 1 // last is inclusive
	}

	return io.NopCloser(bytes.NewReader(data[first:endPos])), nil
}

func (m *BlobStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.blobs, key)
	return nil
}

func (m *BlobStore) Flush(ctx context.Context, ch <-chan *types.Record) (string, int, *api.BlobMeta, []api.IndexEntry, api.Filter, error) {
	var buf bytes.Buffer
	w := m.factory.NewWriter()
	n := 0

	for rec := range ch {
		err := w.Add(rec)
		if err != nil {
			return "", 0, nil, nil, nil, fmt.Errorf("sstable.Add: %w", err)
		}
		n++
	}

	if n == 0 {
		return "", 0, nil, nil, nil, api.ErrNoRecords
	}

	meta, idx, f, err := w.Write(&buf)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("sstable.Write: %w", err)
	}

	key := meta.Filename()

	m.mu.Lock()
	m.blobs[key] = buf.Bytes()
	m.mu.Unlock()

	return key, n, meta, idx, f, nil
}
