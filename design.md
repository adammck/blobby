# Blobby Tombstone Delete Design Document

## 1. Overview

This document outlines a design for implementing logical delete operations in Blobby via tombstone records. The implementation preserves the system's append-only architecture while allowing clients to mark records as deleted. This design builds on the existing versioning approach, treating deletes as a special version that masks previous versions.

## 2. Design Principles

- **Immutable records**: Maintain the append-only architecture by implementing deletion as a special record type
- **Consistent semantics**: Ensure deleted keys behave as if they don't exist from API perspective
- **Simple implementation**: Keep changes minimal and focused on core functionality
- **Compaction aware**: Enable compaction to prune tombstones and deleted records over time

## 3. System Components

### 3.1 Extended Record Structure

```go
type Record struct {
    Key       string    `bson:"key"`
    Timestamp time.Time `bson:"ts"`
    Document  []byte    `bson:"doc,omitempty"`
    Tombstone bool      `bson:"tombstone"`
}
```

**Why add Tombstone?** The tombstone field explicitly marks a record as a deletion marker rather than a data record. A tombstone record has the same Key as the record it's deleting, but contains no document data.

**Why make Document optional?** The Document field is now explicitly marked as omitempty since tombstone records won't contain document data.

### 3.2 Interface Extensions

```go
type Blobby interface {
    // Existing methods
    Get(ctx context.Context, key string) ([]byte, *GetStats, error)
    Put(ctx context.Context, key string, value []byte) (string, error)
    
    // New delete method
    Delete(ctx context.Context, key string) (string, error)
}
```

## 4. Core Workflows

### 4.1 Delete Operation

```go
func (b *Blobby) Delete(ctx context.Context, key string) (string, error)
```

1. Create a tombstone record with:
   - The specified key
   - Current timestamp
   - Tombstone flag set to true
   - No document data
2. Write the tombstone record to the active memtable
3. Return the destination (memtable name)

**Why use the memtable?** Following the same pattern as Put operations ensures consistency and maintains the append-only architecture.

**Why return a destination?** Keeping the same return signature as Put allows for consistent error handling and tracing.

### 4.2 Modified Memtable Interface

```go
func (mt *Memtable) Put(ctx context.Context, key string, value []byte, tombstone bool) (string, error)
```

The memtable needs to be extended to support tombstone records.

### 4.3 Modified Read Path

```go
func (b *Blobby) Get(ctx context.Context, key string) ([]byte, *GetStats, error)
```

The Get method needs to be modified to:
1. Check for tombstone records when scanning
2. Return nil (as if key doesn't exist) when a tombstone is found
3. Avoid returning older versions when newer tombstone exists

**Why return nil instead of NotFound?** This maintains consistency with the current behavior of returning nil when a key isn't found.

## 5. Compaction Considerations

### 5.1 Compaction Behavior

Compaction should be updated to:
1. Keep track of all keys with tombstones
2. Preserve tombstone records
3. Drop all older versions of keys that have tombstones

**Why keep tombstones?** Retaining tombstones ensures that deleted records remain deleted, even if older sstables are read.

### 5.2 Future Enhancements

Future versions could implement:
1. Time-based expiration of tombstones
2. Configurable retention policies for deleted data
3. Compaction strategies optimized for high-delete workloads

## 6. Implementation Strategy

1. **Extend Record Type**
   - Add Tombstone field to Record struct
   - Update serialization/deserialization

2. **Modify Memtable**
   - Update Put to accept tombstone parameter
   - Implement Delete in Blobby to call Put with tombstone=true

3. **Update Read Path**
   - Modify Get to check for tombstones
   - Return nil when a tombstone is found

4. **Update Compaction**
   - Enhance compaction to preserve tombstones
   - Filter out deleted records during compaction

## 7. Testing Strategy

Key tests to implement:
- Basic delete functionality
- Delete and subsequent get behavior
- Delete of non-existent keys
- Delete followed by putting new value
- Compaction with deleted records
- Performance under high tombstone count

## 8. Limitations and Future Enhancements

- No immediate garbage collection of deleted data; relies on compaction
- No configurable retention policy for deleted records
- Tombstones are kept indefinitely
- Future work could include time-based expiration of tombstones
