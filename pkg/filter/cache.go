package filter

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/blobby/pkg/api"
)

type cacheEntry struct {
	info   api.FilterInfo
	expiry time.Time
}

type FilterCache struct {
	store     api.FilterStore
	cache     map[string]cacheEntry
	mu        sync.RWMutex
	ttl       time.Duration
	maxSize   int // max number of filters to cache
	hitCount  int64
	missCount int64
}

func NewFilterCache(store api.FilterStore, ttl time.Duration, maxSize int) *FilterCache {
	return &FilterCache{
		store:   store,
		cache:   make(map[string]cacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

func (c *FilterCache) Get(ctx context.Context, filename string) (api.FilterInfo, error) {
	// Check cache first with read lock
	c.mu.RLock()
	entry, ok := c.cache[filename]
	c.mu.RUnlock()

	if ok && time.Now().Before(entry.expiry) {
		atomic.AddInt64(&c.hitCount, 1)
		return entry.info, nil
	}

	atomic.AddInt64(&c.missCount, 1)

	// Cache miss or expired, fetch from store
	filter, err := c.store.Get(ctx, filename)
	if err != nil {
		return api.FilterInfo{}, err
	}

	// Update cache with write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if we're at capacity
	if len(c.cache) >= c.maxSize {
		c.evict()
	}

	c.cache[filename] = cacheEntry{
		info:   filter,
		expiry: time.Now().Add(c.ttl),
	}

	return filter, nil
}

func (c *FilterCache) Put(ctx context.Context, filename string, filter api.FilterInfo) error {
	// Update the store
	if err := c.store.Put(ctx, filename, filter); err != nil {
		return err
	}

	// Update the cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if we're at capacity
	if len(c.cache) >= c.maxSize && c.cache[filename].info.Data == nil {
		c.evict()
	}

	c.cache[filename] = cacheEntry{
		info:   filter,
		expiry: time.Now().Add(c.ttl),
	}

	return nil
}

func (c *FilterCache) Delete(ctx context.Context, filename string) error {
	// Delete from store
	if err := c.store.Delete(ctx, filename); err != nil {
		return err
	}

	// Delete from cache
	c.mu.Lock()
	delete(c.cache, filename)
	c.mu.Unlock()

	return nil
}

func (c *FilterCache) evict() {
	// Simple strategy: evict the oldest entry
	var oldest string
	var oldestTime time.Time

	for filename, entry := range c.cache {
		if oldest == "" || entry.expiry.Before(oldestTime) {
			oldest = filename
			oldestTime = entry.expiry
		}
	}

	if oldest != "" {
		delete(c.cache, oldest)
	}
}

// Metrics returns cache hit/miss statistics
func (c *FilterCache) Metrics() (hits, misses int64) {
	return atomic.LoadInt64(&c.hitCount), atomic.LoadInt64(&c.missCount)
}
