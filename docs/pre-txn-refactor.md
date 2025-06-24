# Blobby Pre-Transaction Improvements

## Overview

This document outlines critical improvements needed before implementing transaction support in Blobby. Issues are categorized by severity and include detailed implementation guidance.

## Why These Matter for Transactions

Transactions require:
- **Correct MVCC semantics** - the Get() bug breaks isolation
- **Atomic operations** - non-atomic compaction breaks durability  
- **Resource management** - leaks/limits affect transaction throughput
- **Version control** - GC policies interact with transaction visibility

## 1. Correctness Bugs (Critical)

### 1.1 Multi-Version Read Bug in Get()

**Problem**: The `Get()` method returns the first record found without checking if newer versions exist in later SSTables. This violates MVCC semantics when compaction creates overlapping key ranges.

**Location**: `pkg/blobby/archive.go:310`

**Why it matters**: Silent data corruption - readers may see old data when newer versions exist. This breaks transaction isolation levels.

**Test case to demonstrate bug**:
```go
func TestGetMultiVersionBug(t *testing.T) {
    ctx := context.Background()
    c := clockwork.NewFakeClock()
    b := setupBlobby(t, c)
    
    // Write v1 and flush
    c.Advance(1 * time.Second)
    b.Put(ctx, "key1", []byte("v1"))
    b.Put(ctx, "other1", []byte("data"))
    b.Flush(ctx)
    
    // Write v2 and flush  
    c.Advance(1 * time.Hour)
    b.Put(ctx, "key1", []byte("v2"))
    b.Put(ctx, "other2", []byte("data"))
    b.Flush(ctx)
    
    // Write v3 and flush
    c.Advance(1 * time.Hour)
    b.Put(ctx, "key1", []byte("v3"))
    b.Put(ctx, "other3", []byte("data"))
    b.Flush(ctx)
    
    // Compact only first and third sstables
    stats, _ := b.Compact(ctx, api.CompactionOptions{
        Order:    api.OldestFirst,
        MaxFiles: 2,
        // This will compact sstables 1 & 3, leaving 2 alone
    })
    
    // BUG: Get returns v1 from compacted sstable instead of v2
    val, _, _ := b.Get(ctx, "key1")
    require.Equal(t, []byte("v2"), val) // FAILS - gets v1!
}
```

**Fix approach**:
```go
// Current (broken) code:
if rec != nil {
    stats.Source = meta.Filename()
    if rec.Tombstone {
        return nil, stats, &api.NotFound{Key: key}
    }
    return rec.Document, stats, nil  // BUG: returns immediately
}

// Fixed version:
type candidate struct {
    rec    *types.Record
    source string
}

var best *candidate

for _, meta := range metas {
    // ... existing code to find record ...
    if rec != nil {
        if best == nil || rec.Timestamp.After(best.rec.Timestamp) {
            best = &candidate{rec: rec, source: meta.Filename()}
        }
    }
}

if best != nil {
    stats.Source = best.source
    if best.rec.Tombstone {
        return nil, stats, &api.NotFound{Key: key}
    }
    return best.rec.Document, stats, nil
}
```

**Difficulty**: Medium (2-3 days) - requires careful testing of edge cases

**Validation criteria**:
- Test passes with overlapping sstable ranges
- No performance regression on Get()
- Works correctly with tombstones

### 1.2 Non-Atomic Compaction Operations

**Problem**: Compaction performs multiple operations that aren't atomic:
1. Insert new metadata
2. Put index  
3. Put filter
4. Delete old metadata entries
5. Delete old blobs

A crash between any of these leaves the system inconsistent.

**Location**: `pkg/compactor/compactor.go:120-180`

**Why it matters**: Data loss or query failures after crashes.

**Fix approach**:
```go
// Add rollback capability
type compactionTx struct {
    newMeta    *api.BlobMeta
    oldMetas   []*api.BlobMeta
    completed  []string  // track completed operations
}

func (c *Compactor) CompactWithRollback(ctx context.Context, cc *Compaction) error {
    tx := &compactionTx{oldMetas: cc.Inputs}
    
    // Phase 1: Write new data (can be retried)
    tx.newMeta, idx, filter = c.writeCompactedData(ctx, cc)
    tx.completed = append(tx.completed, "write_data")
    
    // Phase 2: Atomic metadata swap (use MongoDB transaction)
    err := c.atomicMetadataSwap(ctx, tx)
    if err != nil {
        return c.rollback(ctx, tx)
    }
    
    // Phase 3: Cleanup old data (safe to fail)
    c.cleanupOldData(ctx, tx.oldMetas)
    return nil
}
```

**Difficulty**: High (1 week) - requires MongoDB transactions or two-phase commit

**Validation criteria**:
- Chaos test with crashes at each step shows no inconsistency
- Rollback successfully cleans up partial compactions
- No orphaned blobs or indices after failures

### 1.3 Race Condition in Memtable Dropping

**Problem**: Time-of-check-time-of-use bug between `CanDropMemtable()` and `Drop()`.

**Location**: `pkg/blobby/archive.go:640-650`

**Why it matters**: Can drop memtables with active scans, causing "collection does not exist" errors.

**Test to demonstrate race**:
```go
func TestMemtableDropRace(t *testing.T) {
    ctx := context.Background()
    b := setupBlobby(t)
    
    // Create data and flush
    b.Put(ctx, "key1", []byte("value1"))
    stats, _ := b.Flush(ctx)
    oldMemtable := stats.FlushedMemtable
    
    // Start a scan that holds reference
    iter, _, _ := b.Scan(ctx, "", "")
    
    // In another goroutine, try to drop
    dropped := make(chan bool)
    go func() {
        // This passes CanDrop check
        canDrop, _ := b.mt.CanDropMemtable(ctx, oldMemtable)
        require.True(t, canDrop)  // PASSES
        
        // But between check and drop, main thread continues scan...
        time.Sleep(10 * time.Millisecond)
        
        err := b.mt.Drop(ctx, oldMemtable)
        dropped <- (err == nil)
    }()
    
    // Continue scan, should fail
    for iter.Next(ctx) {
        // This fails with "collection does not exist"
    }
    
    <-dropped
    require.Error(t, iter.Err()) // Collection was dropped mid-scan!
}
```

**Fix approach**:
```go
// Add atomic drop operation to memtable
func (mt *Memtable) TryDrop(ctx context.Context, name string) error {
    handle, err := mt.GetHandle(ctx, name)
    if err != nil {
        return err
    }
    
    // Atomic check-and-drop under lock
    mt.handlesMu.Lock()
    defer mt.handlesMu.Unlock()
    
    if !handle.CanDrop() {
        return ErrStillReferenced
    }
    
    // Drop while holding lock
    err = mt.dropLocked(ctx, name)
    delete(mt.handles, name)
    return err
}
```

**Difficulty**: Medium - requires careful lock ordering

## 2. Resource Leaks

### 2.1 Unbounded Index/Filter Cache Growth

**Problem**: The `indexes` and `filters` maps grow without bound.

**Location**: `pkg/blobby/archive.go:40-44`

**Why it matters**: OOM on long-running instances.

**Fix approach**:
```go
// Add simple LRU cache
type lruCache struct {
    capacity int
    entries  map[string]*list.Element
    order    *list.List
    mu       sync.Mutex
}

// Or use groupcache/lru:
import "github.com/golang/groupcache/lru"

func (b *Blobby) getIndex(ctx context.Context, fn string) (*index.Index, error) {
    b.indexCache.Lock()
    defer b.indexCache.Unlock()
    
    if ix, ok := b.indexCache.Get(fn); ok {
        return ix.(*index.Index), nil
    }
    
    // Load and cache with eviction
    ix := loadIndex(ctx, fn)
    b.indexCache.Add(fn, ix)
    return ix, nil
}
```

**Difficulty**: Easy - many LRU implementations available

### 2.2 Fragile Scan Cleanup

**Problem**: Complex cleanup logic with `needsCleanup` flag and panic recovery.

**Location**: `pkg/blobby/archive.go:415-445`

**Why it matters**: Resource leaks if cleanup fails.

**Fix approach**:
```go
// Use explicit cleanup type
type scanResources struct {
    handles   []*memtable.Handle
    iterators []api.Iterator
}

func (sr *scanResources) Close() error {
    var errs []error
    
    for _, h := range sr.handles {
        h.Release()
    }
    
    for _, it := range sr.iterators {
        if err := it.Close(); err != nil {
            errs = append(errs, err)
        }
    }
    
    return errors.Join(errs...)
}

// In Scan():
resources := &scanResources{}
defer resources.Close()  // always runs
```

**Difficulty**: Easy - cleaner pattern

### 2.3 Unbounded Memtable Growth

**Problem**: No size limits on active memtable.

**Location**: `pkg/memtable/memtable.go`

**Why it matters**: Can OOM before manual flush.

**Fix approach**:
```go
type Memtable struct {
    // ... existing fields ...
    sizeEstimate int64
    maxSize      int64  // e.g., 64MB
}

func (mt *Memtable) Put(ctx context.Context, key string, value []byte) (string, error) {
    estimatedSize := int64(len(key) + len(value) + 100)  // overhead
    
    if atomic.LoadInt64(&mt.sizeEstimate) + estimatedSize > mt.maxSize {
        return "", ErrMemtableFull
    }
    
    // ... existing put logic ...
    
    atomic.AddInt64(&mt.sizeEstimate, estimatedSize)
    return dest, nil
}
```

**Difficulty**: Medium - need size tracking and auto-flush trigger

## 3. Missing Functionality

### 3.1 No Version Garbage Collection

**Problem**: Compaction keeps all versions forever.

**Location**: `pkg/compactor/compactor.go`

**Why it matters**: Unbounded storage growth.

**Fix approach**:
```go
type GCPolicy struct {
    MaxVersions    int           // keep N versions
    MaxAge         time.Duration // delete older than
    DeleteTombstoneAge time.Duration // remove tombstones after
}

func applyGCPolicy(policy GCPolicy, records []*types.Record) []*types.Record {
    var result []*types.Record
    versionCount := make(map[string]int)
    
    for _, rec := range records {
        // Skip if too old
        if policy.MaxAge > 0 && time.Since(rec.Timestamp) > policy.MaxAge {
            continue
        }
        
        // Skip if too many versions
        if policy.MaxVersions > 0 {
            versionCount[rec.Key]++
            if versionCount[rec.Key] > policy.MaxVersions {
                continue
            }
        }
        
        // Skip old tombstones
        if rec.Tombstone && time.Since(rec.Timestamp) > policy.DeleteTombstoneAge {
            continue
        }
        
        result = append(result, rec)
    }
    
    return result
}
```

**Difficulty**: Medium - need careful policy design

### 3.2 No Concurrency Limits

**Problem**: Unlimited concurrent operations can exhaust resources.

**Location**: Throughout codebase

**Why it matters**: System instability under load.

**Fix approach**:
```go
type Blobby struct {
    // ... existing fields ...
    flushSem    *semaphore.Weighted  // limit concurrent flushes
    compactSem  *semaphore.Weighted  // limit concurrent compactions
    scanSem     *semaphore.Weighted  // limit concurrent scans
}

func (b *Blobby) Flush(ctx context.Context) (*api.FlushStats, error) {
    if err := b.flushSem.Acquire(ctx, 1); err != nil {
        return nil, err
    }
    defer b.flushSem.Release(1)
    
    // ... existing flush logic ...
}
```

**Difficulty**: Easy - use golang.org/x/sync/semaphore

### 3.3 Inconsistent Stats

**Problem**: `Put` returns no stats, `ScanStats.RecordsReturned` only accurate after iteration.

**Location**: Various API methods

**Why it matters**: Hard to debug performance issues.

**Fix approach**:
```go
type PutStats struct {
    Destination   string
    WriteLatency  time.Duration
    RetryCount    int
}

func (b *Blobby) Put(ctx context.Context, key string, value []byte) (string, *PutStats, error) {
    start := time.Now()
    stats := &PutStats{}
    
    dest, err := b.mt.PutWithStats(ctx, key, value, stats)
    stats.Destination = dest
    stats.WriteLatency = time.Since(start)
    
    return dest, stats, err
}
```

**Difficulty**: Easy - add stats structs

## 4. Simplification Opportunities

### 4.1 Duplicate MongoDB Connection Code

**Problem**: `metadata.Store` duplicates connection logic from `memtable`.

**Location**: `pkg/metadata/metadata.go:30-50` and `pkg/memtable/memtable.go:220-240`

**Fix approach**:
```go
// Create shared mongo package
package mongo

type Client struct {
    url string
    db  *mongo.Database
    mu  sync.Mutex
}

func NewClient(url string) *Client {
    return &Client{url: url}
}

func (c *Client) GetDB(ctx context.Context) (*mongo.Database, error) {
    // Shared connection logic
}
```

**Difficulty**: Easy - extract common code

### 4.2 Inconsistent Error Wrapping

**Problem**: Mix of error wrapping styles throughout codebase.

**Fix approach**:
```go
// Standardize on fmt.Errorf with %w
return fmt.Errorf("memtable.Get: %w", err)

// Add context consistently
return fmt.Errorf("get key %q from %s: %w", key, source, err)
```

**Difficulty**: Easy - mechanical change

### 4.3 Common Iterator Patterns

**Problem**: Similar iteration code repeated across scan/get/compact.

**Fix approach**:
```go
// Extract common pattern
func IterateRecords(ctx context.Context, iter api.Iterator, fn func(*types.Record) error) error {
    defer iter.Close()
    
    for iter.Next(ctx) {
        rec := &types.Record{
            Key:       iter.Key(),
            Value:     iter.Value(),
            Timestamp: iter.Timestamp(),
        }
        
        if err := fn(rec); err != nil {
            return err
        }
    }
    
    return iter.Err()
}
```

**Difficulty**: Easy - extract helper functions

## Implementation Priority

1. **Fix Get() multi-version bug** (Critical, 2-3 days)
   - Silent data corruption is worst possible bug
   - Blocks any production use
   - Required for transaction isolation

2. **Fix memtable drop race** (High, 1-2 days)
   - Causes visible errors in production
   - Relatively simple fix with TryDrop pattern
   - Test with concurrent scan workload

3. **Add resource limits** (High, 3-4 days)
   - Prevents OOM in production
   - Start with simple memory limits
   - Add LRU cache for indices/filters

4. **Add atomic compaction** (Medium, 1 week)
   - Complex but critical for durability
   - Consider using MongoDB transactions
   - Can defer if not running compactions yet

5. **Add GC policy** (Medium, 3-4 days)
   - Prevents unbounded growth
   - Start with simple version count limit
   - Critical before transactions (which keep more versions)

6. **Simplify code** (Low, ongoing)
   - Improve maintainability
   - Do during other fixes
   - Extract common patterns

## Success Metrics

Track these to validate improvements:
- Zero data corruption in 1M operation chaos test
- No OOM with 1GB heap under load
- Scan latency <10ms for 1K key range
- Compaction completes atomically (kill -9 safe)

## Testing Requirements

Each fix needs:
- Unit test demonstrating the bug
- Integration test for the fix
- Chaos test coverage where applicable
- Benchmark for performance impact

## How to Approach These Fixes

For junior developers:

1. **Start with the tests** - Write the failing test case first to understand the bug
2. **Use the debugger** - Step through the problematic code path
3. **Make minimal changes** - Don't refactor while fixing
4. **Run existing tests** - Ensure no regressions
5. **Add comprehensive tests** - Cover edge cases
6. **Get code review early** - Share WIP PRs for feedback

### Example PR Structure
```
fix: prevent returning stale data from Get() with overlapping sstables

The Get() method would return the first matching record without checking
for newer versions in subsequent sstables. This violates MVCC semantics.

Changes:
- Scan all candidate sstables before returning
- Track newest version by timestamp  
- Add test case demonstrating the bug

Fixes #XXX
```