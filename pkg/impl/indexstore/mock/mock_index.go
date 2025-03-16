package mock

import (
	"context"

	"github.com/adammck/blobby/pkg/api"
)

type MockIndexStore struct {
	Contents map[string]api.Index
}

func New() *MockIndexStore {
	return &MockIndexStore{
		Contents: make(map[string]api.Index),
	}
}

func (m *MockIndexStore) Put(ctx context.Context, filename string, entries api.Index) error {
	m.Contents[filename] = entries
	return nil
}

func (m *MockIndexStore) Get(ctx context.Context, filename string) (api.Index, error) {
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
