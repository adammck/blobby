package mock

import (
	"context"

	"github.com/adammck/blobby/pkg/api"
)

type MockIndexStore struct {
	Contents map[string][]api.IndexEntry
}

func New() *MockIndexStore {
	return &MockIndexStore{
		Contents: make(map[string][]api.IndexEntry),
	}
}

func (m *MockIndexStore) Put(ctx context.Context, filename string, entries []api.IndexEntry) error {
	m.Contents[filename] = entries
	return nil
}

func (m *MockIndexStore) Get(ctx context.Context, filename string) ([]api.IndexEntry, error) {
	entries, ok := m.Contents[filename]
	if !ok {
		return nil, &api.IndexNotFound{Filename: filename}
	}
	return entries, nil
}

func (m *MockIndexStore) Delete(ctx context.Context, filename string) error {
	delete(m.Contents, filename)
	return nil
}
