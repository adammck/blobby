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

// Manager is a high-level interface for reading and writing sstables. It wraps
// the blobstore, but reads and writes Records, not raw bytes. (It doesn't help
// with byte offsets; see api.IndexStore for that.)
type Manager struct {
	bs api.BlobStore
	c  clockwork.Clock

	// TODO(adammck): Can we get rid of this?
	f Factory
}

func NewManager(bs api.BlobStore, c clockwork.Clock, f Factory) *Manager {
	return &Manager{
		bs: bs,
		c:  c,
		f:  f,
	}
}

// GetFull reads an entire SST, one record at a time. The caller MUST close the
// Reader to avoid leaking resources.
func (m *Manager) GetFull(ctx context.Context, key string) (*Reader, error) {
	rc, err := m.bs.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("BlobStore.Get: %w", err)
	}

	r, err := NewReader(rc)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}

	return r, nil
}

// GetRange reads a specific byte range of an SST, one record at a time. See
// api.IndexStore to determine appropriate offsets. The caller MUST close the
// Reader to avoid leaking resources.
func (m *Manager) GetRange(ctx context.Context, key string, first, last int64) (*Reader, error) {
	if first == 0 {
		return nil, fmt.Errorf("first must be greater than zero; use GetFull")
	}

	rc, err := m.bs.GetRange(ctx, key, first, last)
	if err != nil {
		return nil, fmt.Errorf("BlobStore.GetRange: %w", err)
	}

	return NewPartialReader(rc), nil
}

// Delete removes the SST with the given key.
func (m *Manager) Delete(ctx context.Context, key string) error {
	return m.bs.Delete(ctx, key)
}

// Flush creates a new SST by draining the given channel of Records.
func (m *Manager) Flush(ctx context.Context, ch <-chan *types.Record) (dest string, meta *api.BlobMeta, index []api.IndexEntry, filter filter.Filter, err error) {
	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w := m.f.NewWriter()
	n := 0
	for rec := range ch {
		err = w.Add(rec)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("sstable.Writer.Add: %w", err)
		}
		n++
	}

	if n == 0 {
		return "", nil, nil, nil, ErrNoRecords
	}

	meta, index, filter, err = w.Write(f)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("sstable.Writer.Write: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("File.Seek: %w", err)
	}

	key := meta.Filename()
	err = m.bs.Put(ctx, key, f)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("BlobStore.Put: %w", err)
	}

	return key, meta, index, filter, nil
}
