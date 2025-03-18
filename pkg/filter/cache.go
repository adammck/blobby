package filter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter/xor"
)

func newFilterFromInfo(info api.FilterInfo) (Filter, error) {
	switch info.Type {
	case xor.FilterType:
		return xor.New(info)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", info.Type)
	}
}

type cacheEntry struct {
	filter Filter
	info   api.FilterInfo
	expiry time.Time
}

type FilterCache struct {
	store     api.FilterStore
	cache     map[string]cacheEntry
	mu        sync.RWMutex
	ttl       time.Duration
	maxSize   int
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
	c.mu.RLock()
	entry, ok := c.cache[filename]
	c.mu.RUnlock()

	if ok && time.Now().Before(entry.expiry) {
		atomic.AddInt64(&c.hitCount, 1)
		return entry.info, nil
	}

	atomic.AddInt64(&c.missCount, 1)

	info, err := c.store.Get(ctx, filename)
	if err != nil {
		return api.FilterInfo{}, err
	}

	f, err := newFilterFromInfo(info)
	if err != nil {
		return api.FilterInfo{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) >= c.maxSize {
		c.evict()
	}

	c.cache[filename] = cacheEntry{
		filter: f,
		info:   info,
		expiry: time.Now().Add(c.ttl),
	}

	return info, nil
}

func (c *FilterCache) Put(ctx context.Context, filename string, filter api.FilterInfo) error {
	if err := c.store.Put(ctx, filename, filter); err != nil {
		return err
	}

	f, err := newFilterFromInfo(filter)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) >= c.maxSize && c.cache[filename].info.Data == nil {
		c.evict()
	}

	c.cache[filename] = cacheEntry{
		filter: f,
		info:   filter,
		expiry: time.Now().Add(c.ttl),
	}

	return nil
}

func (c *FilterCache) Delete(ctx context.Context, filename string) error {
	if err := c.store.Delete(ctx, filename); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.cache, filename)
	c.mu.Unlock()

	return nil
}

func (c *FilterCache) evict() {
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

func (c *FilterCache) Metrics() (hits, misses int64) {
	return atomic.LoadInt64(&c.hitCount), atomic.LoadInt64(&c.missCount)
}

func (c *FilterCache) Contains(ctx context.Context, filename, key string) (bool, error) {
	c.mu.RLock()
	entry, ok := c.cache[filename]
	c.mu.RUnlock()

	if !ok || time.Now().After(entry.expiry) {
		if _, err := c.Get(ctx, filename); err != nil {
			return false, err
		}
		c.mu.RLock()
		entry = c.cache[filename]
		c.mu.RUnlock()
	}

	return entry.filter.Contains(key), nil
}
