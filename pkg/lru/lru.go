package lru

import (
	"container/list"
	"sync"
)

// Cache implements a thread-safe LRU cache with a fixed capacity
type Cache struct {
	capacity int
	items    map[string]*list.Element
	order    *list.List
	mu       sync.Mutex
}

// entry represents a key-value pair in the cache
type entry struct {
	key   string
	value interface{}
}

// New creates a new LRU cache with the specified capacity
func New(capacity int) *Cache {
	if capacity <= 0 {
		panic("cache capacity must be positive")
	}
	
	return &Cache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache. Returns the value and true if found,
// or nil and false if not found. Accessing an item moves it to the front.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if elem, exists := c.items[key]; exists {
		// Move to front (most recently used)
		c.order.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}
	
	return nil, false
}

// Put adds or updates a value in the cache. If the cache is at capacity,
// the least recently used item is evicted first.
func (c *Cache) Put(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Update existing item
	if elem, exists := c.items[key]; exists {
		elem.Value.(*entry).value = value
		c.order.MoveToFront(elem)
		return
	}
	
	// Add new item
	elem := c.order.PushFront(&entry{key: key, value: value})
	c.items[key] = elem
	
	// Evict least recently used if over capacity
	if len(c.items) > c.capacity {
		c.evictLRU()
	}
}

// evictLRU removes the least recently used item from the cache
func (c *Cache) evictLRU() {
	elem := c.order.Back()
	if elem != nil {
		c.order.Remove(elem)
		e := elem.Value.(*entry)
		delete(c.items, e.key)
	}
}

// Len returns the current number of items in the cache
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.order.Init()
}