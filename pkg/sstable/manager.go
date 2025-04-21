package sstable

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
)

var ErrNoRecords = errors.New("NoRecords")

type Manager struct {
	store   api.BlobStore
	clock   clockwork.Clock
	factory Factory
}

func NewManager(store api.BlobStore, clock clockwork.Clock, factory Factory) *Manager {
	return &Manager{
		store:   store,
		clock:   clock,
		factory: factory,
	}
}

// GetFull reads a single SSTable from the store.
func (m *Manager) GetFull(ctx context.Context, key string) (*Reader, error) {
	body, err := m.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("Get: %w", err)
	}

	reader, err := NewReader(body)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}

	return reader, nil
}

// GetPartial reads a byte range of an SSTable from the store.
func (m *Manager) GetPartial(ctx context.Context, key string, first, last int64) (*Reader, error) {
	if first == 0 {
		return nil, fmt.Errorf("first must be greater than zero; use GetFull")
	}

	body, err := m.store.GetRange(ctx, key, first, last)
	if err != nil {
		return nil, fmt.Errorf("GetRange: %w", err)
	}

	reader := NewPartialReader(body)
	return reader, nil
}

func (m *Manager) Delete(ctx context.Context, key string) error {
	return m.store.Delete(ctx, key)
}

// Flush writes an SSTable to the store
func (m *Manager) Flush(ctx context.Context, ch <-chan *types.Record) (dest string, count int, meta *api.BlobMeta, index []api.IndexEntry, filter filter.Filter, err error) {
	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w := m.factory.NewWriter()
	n := 0
	for rec := range ch {
		err = w.Add(rec)
		if err != nil {
			return "", 0, nil, nil, nil, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	if n == 0 {
		return "", 0, nil, nil, nil, ErrNoRecords
	}

	meta, index, filter, err = w.Write(f)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("sstable.Write: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("Seek: %w", err)
	}

	key := meta.Filename()
	err = m.store.Put(ctx, key, f)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("Put: %w", err)
	}

	return key, n, meta, index, filter, nil
}
