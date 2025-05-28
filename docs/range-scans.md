# Blobby Range Scan Design Document

## 1. Overview

This document outlines a design for implementing range scans in Blobby. Range scans allow clients to efficiently retrieve multiple records within a specified key range, building on the existing point lookup infrastructure. The implementation provides snapshot isolation, efficient iteration, and seamless handling of concurrent operations.

## 2. Design Principles

- **Iterator-based streaming**: Never load entire result sets into memory
- **Snapshot isolation**: Each scan sees a consistent point-in-time view
- **Minimal memory overhead**: Stream results as they're discovered
- **Simple semantics**: Half-open intervals [start, end) matching Go slices
- **Tombstone-aware**: Correctly skip deleted keys during iteration
- **Version handling**: Return only the latest version of each key

## 3. System Components

### 3.1 Core Iterator Interface

```go
type Iterator interface {
    // Next advances to the next key and returns its value.
    // Returns nil, nil when iteration is complete.
    // Returns nil, error on failures.
    Next() ([]byte, error)
    
    // Key returns the current key, or empty string if iteration hasn't started.
    Key() string
    
    // Close releases resources. Safe to call multiple times.
    Close() error
}
```

**Why this interface?** Separating key retrieval from advancement allows callers to inspect keys without copying values. This matters for large documents where the caller might want to skip based on key patterns.

### 3.2 Extended Blobby Interface

```go
type Blobby interface {
    // Existing methods...
    
    // Scan returns an iterator for keys in [start, end).
    // The iterator sees a consistent snapshot at call time.
    Scan(ctx context.Context, start, end string) (Iterator, error)
    
    // ScanPrefix returns an iterator for all keys with the given prefix.
    ScanPrefix(ctx context.Context, prefix string) (Iterator, error)
}

// ScanPrefix is implemented as:
// return b.Scan(ctx, prefix, prefix + "\xff")
```

**Why ScanPrefix?** Prefix scans are extremely common (e.g., "users:*") and this provides a clean API without callers needing to construct end keys.

### 3.3 Compound Iterator

```go
type compoundIterator struct {
    ctx      context.Context
    iters    []sourceIterator
    heap     *iteratorHeap
    
    // Current position
    currentKey   string
    currentValue []byte
    currentErr   error
    
    // For detecting versions of same key
    lastEmittedKey string
}

type sourceIterator interface {
    Next() (*types.Record, error)
    Peek() (*types.Record, error)  // Look at next without advancing
    Source() string                 // For debugging/stats
}
```

**Why compound instead of merge?** Better name - we're combining multiple sources, not merging their contents. The heap maintains ordering across sources while tracking which records we've seen.

### 3.4 Memory Management

```go
// For memtables - prevent deletion during scans
type memtableRef struct {
    handle *memtable.Handle
    count  int32  // atomic reference count
}

// For sstables - immutable, so just track metadata
type sstableRef struct {
    meta     *api.BlobMeta
    filename string
}
```

**Why reference counting for memtables only?** SSTable immutability means we only need to prevent memtable deletion. Reference counting is simpler than keeping transactions open.

## 4. Implementation Details

### 4.1 Scan Initialization

```go
func (b *Blobby) Scan(ctx context.Context, start, end string) (Iterator, error) {
    // Validate inputs
    if start > end && end != "" {
        return nil, fmt.Errorf("invalid range: start=%q > end=%q", start, end)
    }
    
    // Capture consistent snapshot
    b.mu.RLock()
    sstables := b.getSstablesInRange(start, end)
    memtables := b.getActiveMemtables()
    b.mu.RUnlock()
    
    // Create source iterators
    var sources []sourceIterator
    
    // Add memtable iterators (newest to oldest)
    for i := len(memtables) - 1; i >= 0; i-- {
        iter := newMemtableIterator(ctx, memtables[i], start, end)
        sources = append(sources, iter)
    }
    
    // Add sstable iterators (newest to oldest)
    for _, sst := range sstables {
        iter, err := b.newSstableIterator(ctx, sst, start, end)
        if err != nil {
            // Clean up already-created iterators
            for _, s := range sources {
                s.Close()
            }
            return nil, err
        }
        sources = append(sources, iter)
    }
    
    return newCompoundIterator(ctx, sources), nil
}
```

**Why this order?** Newer sources first means we encounter the latest version of each key first, simplifying version skipping.

### 4.2 Compound Iterator Logic

```go
func (c *compoundIterator) Next() ([]byte, error) {
    // Check context cancellation
    select {
    case <-c.ctx.Done():
        return nil, c.ctx.Err()
    default:
    }
    
    for {
        rec, err := c.nextRecord()
        if err != nil {
            return nil, err
        }
        if rec == nil {
            return nil, nil  // EOF
        }
        
        // Skip if we've seen this key (older version)
        if rec.Key == c.lastEmittedKey {
            continue
        }
        
        // Skip if tombstone
        if rec.Tombstone {
            c.lastEmittedKey = rec.Key
            continue
        }
        
        // Found a live record
        c.currentKey = rec.Key
        c.currentValue = rec.Document
        c.lastEmittedKey = rec.Key
        return rec.Document, nil
    }
}

func (c *compoundIterator) nextRecord() (*types.Record, error) {
    if c.heap.Len() == 0 {
        return nil, nil  // EOF
    }
    
    // Get next record from heap
    state := heap.Pop(c.heap).(*iterState)
    rec := state.current
    
    // Advance that iterator
    next, err := state.iter.Next()
    if err != nil {
        return nil, err
    }
    
    // Re-add to heap if not exhausted
    if next != nil {
        state.current = next
        heap.Push(c.heap, state)
    }
    
    return rec, nil
}
```

**Why track lastEmittedKey?** This ensures we skip older versions efficiently without needing to compare timestamps.

### 4.3 Heap Implementation

```go
type iteratorHeap []*iterState

type iterState struct {
    iter    sourceIterator
    current *types.Record
}

func (h iteratorHeap) Less(i, j int) bool {
    // First by key (ascending)
    cmp := strings.Compare(h[i].current.Key, h[j].current.Key)
    if cmp != 0 {
        return cmp < 0
    }
    
    // Then by timestamp (descending) for same key
    return h[i].current.Timestamp.After(h[j].current.Timestamp)
}
```

**Why timestamp descending?** We want the newest version first, so we can skip older versions without buffering.

### 4.4 Optimizations

```go
// Bloom filter checking for range scans
func (b *Blobby) mightContainRange(filter BloomFilter, start, end string) bool {
    // Cannot definitively exclude ranges with bloom filters
    // But can check a sample of keys in the range
    samples := generateKeySamples(start, end, 10)
    for _, key := range samples {
        if filter.Contains(key) {
            return true
        }
    }
    return false  // Probably doesn't contain range
}
```

**Why sample keys?** Bloom filters can't directly answer range queries, but sampling can catch obviously empty ranges.

## 5. Correctness Considerations

### 5.1 Snapshot Isolation Guarantees

- All data committed before scan starts is visible
- No data committed after scan starts is visible  
- Scan results remain consistent even as writes continue

### 5.2 Edge Cases

```go
// Empty range
Scan(ctx, "b", "b") // Returns empty iterator

// Entire keyspace  
Scan(ctx, "", "") // Returns all keys

// Unicode boundaries
ScanPrefix(ctx, "users:") // Correctly handles multi-byte UTF-8
```

### 5.3 Resource Cleanup

```go
// Iterator must be closed even after errors
iter, err := b.Scan(ctx, start, end)
if err != nil {
    return err
}
defer iter.Close()  // Always runs

// Context cancellation stops iteration
for {
    val, err := iter.Next()
    if err != nil {
        return err  // Could be ctx.Err()
    }
    if val == nil {
        break
    }
    // Process...
}
```

## 6. Testing Strategy

### 6.1 Unit Tests

```go
func TestScanEmptyRange(t *testing.T)
func TestScanPrefix(t *testing.T) 
func TestScanWithTombstones(t *testing.T)
func TestScanMultipleVersions(t *testing.T)
func TestScanContextCancellation(t *testing.T)
func TestScanResourceCleanup(t *testing.T)
func TestScanSnapshot(t *testing.T)  // Writes during scan don't affect results
```

### 6.2 Chaos Tests

Add to existing chaos test framework:

```go
type scanOperation struct {
    startKey string
    endKey   string
}

func (o scanOperation) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
    iter, err := b.Scan(ctx, o.startKey, o.endKey)
    if err != nil {
        return err
    }
    defer iter.Close()
    
    var count int
    for {
        val, err := iter.Next()
        if err != nil {
            return err
        }
        if val == nil {
            break
        }
        
        // Verify against model
        key := iter.Key()
        expected, exists := state.model[key]
        if !exists || !bytes.Equal(val, expected) {
            return fmt.Errorf("mismatch at %s", key)
        }
        count++
    }
    
    t.Logf("Scanned %d keys in [%s, %s)", count, o.startKey, o.endKey)
    return nil
}
```

### 6.3 Performance Benchmarks

```go
func BenchmarkScanFullKeyspace(b *testing.B)
func BenchmarkScanNarrowRange(b *testing.B)
func BenchmarkScanWithManyVersions(b *testing.B)
func BenchmarkScanPrefixVsRange(b *testing.B)
```

## 7. Future Enhancements

### 7.1 Limit Iterator

```go
type limitIterator struct {
    inner Iterator
    limit int
    count int
}

func WithLimit(iter Iterator, limit int) Iterator {
    return &limitIterator{inner: iter, limit: limit}
}
```

**Why separate?** Keeps the core iterator simple while allowing easy composition.

### 7.2 Reverse Iteration

Would require:
- Reverse ordering in the heap
- Seeking to end of range in sstables
- Complexity in handling versions (need newest-first still)

**Why defer?** Adds significant complexity for unclear benefit.

### 7.3 Parallel Range Scans

For very large ranges:
- Split into sub-ranges
- Scan in parallel
- Merge results

**Why not now?** Single-threaded is simpler and likely sufficient.

## 8. Implementation Order

1. Basic iterator interface and tests
2. Memtable iterator (simplest - uses MongoDB queries)
3. SSTable iterator (wraps existing reader)
4. Compound iterator with heap-based merging
5. Snapshot isolation with reference counting
6. ScanPrefix convenience method
7. Context cancellation support
8. Chaos test integration

This order allows incremental progress with testable milestones.
