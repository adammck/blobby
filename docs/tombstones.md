# Blobby Tombstone Design Document

## 1. Overview

This document outlines a design for adding logical deletion to Blobby using tombstone records. While the system already supports overwriting records with new versions, there's currently no way to mark a key as explicitly deleted. Tombstones will allow users to logically delete records while preserving Blobby's append-only architecture, providing proper semantics for deletion operations.

## 2. Design Principles

- **Maintain append-only architecture**: Deletions should be implemented as special records with a tombstone flag rather than physically removing data
- **Clear semantics**: Get operations on deleted keys should return a dedicated NotFound error
- **Efficiency**: Minimize overhead for implementing tombstones
- **Simplicity**: Implement only logical deletion without garbage collection in this phase
- **Consistency**: Treat deleted keys and non-existent keys identically from an API perspective

## 3. Data Structure Changes

### 3.1 Record Structure

```go
type Record struct {
    Key       string    `bson:"key"`
    Timestamp time.Time `bson:"ts"`
    Document  []byte    `bson:"doc"`
    Tombstone bool      `bson:"tombstone,omitempty"` // New field
}
```

**Why boolean flag?** 
- Simple, minimal storage overhead (1 bit when encoded)
- Clear intent - a record is either a tombstone or it isn't
- Easy to check in query logic

## 4. API Changes

### 4.1 Blobby Interface Extension

```go
type DeleteStats struct {
    Destination string    // The memtable where the tombstone was written
    Timestamp   time.Time // When the delete occurred
}

type NotFound struct {
    Key string
}

func (e *NotFound) Error() string {
    return fmt.Sprintf("key not found: %s", e.Key)
}

func (e *NotFound) Is(err error) bool {
    _, ok := err.(*NotFound)
    return ok
}

type Blobby interface {
    // Existing methods with updated behavior
    Put(ctx context.Context, key string, value []byte) (string, error)
    Get(ctx context.Context, key string) ([]byte, *GetStats, error) // Now returns NotFound for deleted/missing keys
    Flush(ctx context.Context) (*FlushStats, error)
    Compact(ctx context.Context, opts CompactionOptions) ([]*CompactionStats, error)
    
    // New method
    Delete(ctx context.Context, key string) (*DeleteStats, error)
}
```

**Why DeleteStats?** 
- More extensible than returning a primitive string
- Parallels structure of other operations that return stats
- Captures operation metadata useful for debugging and observability

### 4.2 Memtable Changes

```go
func (mt *Memtable) PutRecord(ctx context.Context, rec *types.Record) (string, error)
```

**Implementation strategy:**
- This new method will handle both regular records and tombstones
- Refactor the existing Put method to use PutRecord internally
- Keep the same insertion logic (including retry on timestamp collision)

## 5. Behavior Changes

### 5.1 Get Behavior

For Get operations:
- If the newest record for a key is a tombstone: return nil value with NotFound error
- If no record exists for a key: return nil value with NotFound error
- Return value and nil error only for non-deleted, existing keys

### 5.2 Delete Behavior

For Delete operations:
- Create a tombstone record with empty Document and Tombstone=true
- Write the tombstone to the active memtable
- Return statistics about the operation
- If the key doesn't exist, still create a tombstone (idempotent operation)

### 5.3 Compaction Behavior

During compaction:
- Preserve tombstones exactly like regular records
- No special garbage collection in this phase
- Compacted SSTables will contain tombstones that mask older versions

## 6. Testing Strategy

### 6.1 Unit Tests

```go
// Record tests
func TestRecordSerializationWithTombstone(t *testing.T) // Verify tombstone field is properly serialized/deserialized

// Memtable tests
func TestMemtablePutRecordNormal(t *testing.T) // Test PutRecord with regular record
func TestMemtablePutRecordTombstone(t *testing.T) // Test PutRecord with tombstone
func TestMemtableGetTombstone(t *testing.T) // Test retrieving a tombstone from memtable

// Error tests
func TestNotFoundError(t *testing.T) // Test NotFound error behavior and matching
```

### 6.2 Blobby Tests

```go
func TestDelete(t *testing.T) // Test basic delete returns correct stats
func TestGetDeletedKey(t *testing.T) // Test Get returns NotFound for a deleted key
func TestGetNonExistentKey(t *testing.T) // Test Get returns NotFound for a key that never existed
func TestDeleteNonExistentKey(t *testing.T) // Test deleting a key that doesn't exist succeeds
func TestPutAfterDelete(t *testing.T) // Test that putting a value after deletion makes the key visible again
func TestDeleteAfterPut(t *testing.T) // Test that deleting after a put hides the key
```

### 6.3 Integration Tests

```go
func TestDeleteThenFlush(t *testing.T) // Delete, flush, then verify key is still recognized as deleted
func TestDeleteAcrossMultipleSSTs(t *testing.T) // Create complex scenario with key versions across multiple SSTables, then delete
func TestCompactionWithTombstones(t *testing.T) // Verify tombstones are preserved during compaction
```

### 6.4 Chaos Testing

```go
// Extend existing chaos test to include Delete operations and verify correctness
func chaosDelete(h *testHarness, key string) Op
```

## 7. Implementation Plan

1. **Record Type Changes** (pkg/types/types.go)
   - Add Tombstone field to Record struct
   - Ensure serialization/deserialization handles the new field
   - Add unit tests for Record with tombstone

2. **NotFound Error** (pkg/api/errors.go)
   - Create NotFound error type
   - Add unit tests for error behavior

3. **Memtable Changes** (pkg/memtable/memtable.go)
   - Add PutRecord method 
   - Refactor existing Put to use PutRecord
   - Add unit tests for the new method

4. **Blobby Method Changes** (pkg/blobby/archive.go)
   - Implement Delete method
   - Update Get method to handle tombstones and return NotFound errors
   - Add unit tests for new/changed methods

5. **Scan Method Updates** (pkg/blobby/archive.go)
   - Ensure Scan properly returns tombstone records
   - Update unit tests for Scan

6. **Integration Test Updates**
   - Add integration tests for combined operations (delete+flush, delete+compact)
   - Update existing integration tests to handle tombstones correctly

7. **Compactor Updates** (pkg/compactor/compactor.go)
   - Ensure compaction correctly processes tombstone records
   - Add tests for compaction with tombstones

8. **Chaos Test Updates** (pkg/blobby/archive_chaos_test.go)
   - Add Delete operations to chaos test framework
   - Run chaos tests to verify correctness under random operations

Each step should be committed individually after implementation and testing. Tests might fail temporarily between commits, but the final state should have all tests passing.
