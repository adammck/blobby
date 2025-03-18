package filter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	mu    sync.RWMutex
	store map[string]api.FilterInfo
}

func newMockStore() *mockStore {
	return &mockStore{store: make(map[string]api.FilterInfo)}
}

func (m *mockStore) Init(ctx context.Context) error { return nil }

func (m *mockStore) Put(ctx context.Context, filename string, f api.FilterInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[filename] = f
	return nil
}

func (m *mockStore) Get(ctx context.Context, filename string) (api.FilterInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f, ok := m.store[filename]
	if !ok {
		return api.FilterInfo{}, &api.FilterNotFound{Filename: filename}
	}
	return f, nil
}

func (m *mockStore) Delete(ctx context.Context, filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, filename)
	return nil
}

func TestFilterCache(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	cache := NewFilterCache(store, time.Hour, 2)

	// Create test data
	keys := []string{"key1", "key2", "key3"}
	info, err := Create(keys)
	require.NoError(t, err)

	// Test Put
	err = cache.Put(ctx, "file1.sst", info)
	require.NoError(t, err)

	// Test Get - cache hit
	cachedInfo, err := cache.Get(ctx, "file1.sst")
	require.NoError(t, err)
	require.Equal(t, info.Type, cachedInfo.Type)
	require.Equal(t, info.Data, cachedInfo.Data)

	// Test Contains
	present, err := cache.Contains(ctx, "file1.sst", "key1")
	require.NoError(t, err)
	require.True(t, present)

	notPresent, err := cache.Contains(ctx, "file1.sst", "nonexistent")
	require.NoError(t, err)
	require.False(t, notPresent)

	// Test eviction
	err = cache.Put(ctx, "file2.sst", info)
	require.NoError(t, err)
	err = cache.Put(ctx, "file3.sst", info)
	require.NoError(t, err)

	// file1 should be evicted
	delete(store.store, "file1.sst") // remove from backing store
	_, err = cache.Get(ctx, "file1.sst")
	require.Error(t, err) // should fail since it's not in store anymore

	// Test expiry
	smallCache := NewFilterCache(store, 50*time.Millisecond, 10)
	err = smallCache.Put(ctx, "file1.sst", info)
	require.NoError(t, err)

	_, err = smallCache.Get(ctx, "file1.sst")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond) // wait for expiry

	// Delete from backing store to simulate permanent miss
	delete(store.store, "file1.sst")

	// should be a miss and fail since we deleted it from store
	_, err = smallCache.Get(ctx, "file1.sst")
	require.Error(t, err)

	// Test metrics
	hits, misses := smallCache.Metrics()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses)
}

func TestCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	cache := NewFilterCache(store, time.Hour, 100)

	// Create some test data
	keys := []string{"key1", "key2", "key3"}
	info, err := Create(keys)
	require.NoError(t, err)

	err = cache.Put(ctx, "file.sst", info)
	require.NoError(t, err)

	// Run concurrent Contains operations
	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.Contains(ctx, "file.sst", "key1")
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
