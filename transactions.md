# Blobby Transaction Design Document

## 1. Overview

This document outlines a design for adding transaction support to Blobby. The implementation enables atomic operations across multiple keys while preserving the system's append-only architecture. Transactions will follow optimistic concurrency control semantics with an in-memory transaction manager for the initial implementation.

## 2. Design Principles

- **Immutable records**: Preserve the existing append-only nature of records to maintain simplicity and performance
- **Single-writer assumption**: Initial implementation optimized for a single writer to reduce complexity
- **In-memory transaction state**: Transaction metadata stored in memory to minimize overhead
- **Optimistic concurrency control**: First-committer-wins semantics to maximize concurrency
- **Non-transactional compatibility**: Non-transactional writes treated as implicit transactions
- **Explicit failure modes**: Clear semantics for expected error conditions

## 3. System Components

### 3.1 Transaction Manager

```go
type TxnManager struct {
    mu             sync.RWMutex
    nextTxID       uint64
    transactions   map[string]*Transaction
    currentTs      func() time.Time
    maxTxnLifetime time.Duration
}

type Transaction struct {
    ID          string
    Status      string  // "pending", "committed", "aborted"
    StartTs     time.Time
    CommitTs    time.Time
    LastActive  time.Time
    WriteSet    map[string]struct{}
    Implicit    bool  // true for non-transactional writes
}

func NewTxnManager(clock clockwork.Clock) *TxnManager
func (tm *TxnManager) Begin() string
func (tm *TxnManager) AddToWriteSet(txID, key string) error
func (tm *TxnManager) Commit(txID string) error
func (tm *TxnManager) Abort(txID string) error
func (tm *TxnManager) CreateImplicitTransaction() string
func (tm *TxnManager) GetStatus(txID string) (string, bool)
func (tm *TxnManager) PruneOldTransactions()
```

**Why in-memory?** An in-memory implementation provides minimal overhead for transaction tracking in the single-writer scenario. Since only one writer exists, durability of transaction metadata isn't critical for correctness - if the system crashes, all uncommitted transactions are effectively aborted.

**Why track LastActive?** This enables automatic cleanup of zombie transactions that are started but never explicitly committed or aborted, preventing memory leaks.

**Why track Implicit flag?** Non-transactional writes need to be treated as implicit single-operation transactions to maintain consistent conflict detection.

### 3.2 Modified Record Structure

```go
type Record struct {
    Key       string    `bson:"key"`
    Timestamp time.Time `bson:"ts"`
    Document  []byte    `bson:"doc"`
    TxID      string    `bson:"txid,omitempty"`
    Tombstone bool      `bson:"tombstone,omitempty"` // For future delete support
}
```

**Why add TxID?** The TxID field associates records with their parent transaction without requiring modification after initial write, preserving the append-only architecture.

**Why add Tombstone?** While not immediately implemented, this field allows future support for deletes within transactions by writing tombstone records instead of actually removing data.

### 3.3 Interface Extensions

```go
type Blobby interface {
    // Existing methods
    Get(ctx context.Context, key string) ([]byte, *GetStats, error)
    Put(ctx context.Context, key string, value []byte) (string, error)
    
    // New transaction methods
    BeginTransaction(ctx context.Context) (string, error)
    PutInTransaction(ctx context.Context, txID string, key string, value []byte) (string, error)
    CommitTransaction(ctx context.Context, txID string) error
    AbortTransaction(ctx context.Context, txID string) error
}
```

## 4. Transaction Workflows

### 4.1 Begin Transaction

```go
func (b *Blobby) BeginTransaction(ctx context.Context) (string, error)
```

1. Generate a unique transaction ID
2. Create transaction record with "pending" status and current timestamp
3. Return transaction ID to client

**Why use unique IDs?** Ensures no collision between transaction identifiers, even across process restarts. This prevents accidentally conflating different transactions.

**Why record start timestamp?** The start timestamp defines the transaction's view of the database and is essential for conflict detection.

### 4.2 Write Operations

```go
func (b *Blobby) PutInTransaction(ctx context.Context, txID string, key string, value []byte) (string, error)
```

1. Validate transaction exists and is in "pending" state
2. Update transaction's LastActive timestamp
3. Add key to transaction's write set
4. Write record to memtable with transaction ID attached
5. Return destination to client

**Why track the write set?** The write set is crucial for conflict detection during commit. Without it, we couldn't determine which keys were modified by which transactions.

**Why update LastActive?** This prevents zombie transactions by enabling timeout-based cleanup for abandoned transactions.

### 4.3 Non-Transactional Writes

```go
func (b *Blobby) Put(ctx context.Context, key string, value []byte) (string, error)
```

1. Create an implicit transaction with immediate start timestamp
2. Add key to implicit transaction's write set
3. Write record with the implicit transaction ID
4. Immediately commit the implicit transaction
5. Return destination to client

**Why treat regular writes as implicit transactions?** This ensures consistent handling of concurrency conflicts between transactional and non-transactional writes. Without this approach, non-transactional writes could silently overwrite transactional data or vice versa.

### 4.4 Commit Transaction

```go
func (b *Blobby) CommitTransaction(ctx context.Context, txID string) error
```

1. Validate transaction exists and is in "pending" state
2. Perform conflict detection:
   - Find any committed transactions with commit timestamp > this transaction's start timestamp
   - Check if any of those transactions modified keys in this transaction's write set
   - If conflict found, abort transaction and return error
3. If no conflicts, update transaction status to "committed" with current timestamp

**Why this conflict detection approach?** First-committer-wins semantics ensure consistent resolution of concurrent modifications without requiring locks. This maximizes concurrency for non-conflicting operations.

**Why make commit idempotent?** Clients may retry commit operations after timeouts or errors; idempotent commits ensure consistent behavior in these cases.

### 4.5 Abort Transaction

```go
func (b *Blobby) AbortTransaction(ctx context.Context, txID string) error
```

1. Validate transaction exists
2. Update transaction status to "aborted"

**Why keep aborted transaction metadata?** The read path needs to know which records to skip based on transaction status. Keeping metadata of aborted transactions enables this filtering.

### 4.6 Read Path Changes

```go
func (b *Blobby) Get(ctx context.Context, key string) ([]byte, *GetStats, error)
```

1. Fetch candidate records as before (newest to oldest)
2. For each record:
   - If no TxID, return immediately
   - If has TxID, check transaction status in TxnManager
   - If status="committed", return record
   - If status="pending" or "aborted", skip to next record
   - If tombstone=true and status="committed", return not found
3. If no visible record found, return not found

**Why filter based on transaction status?** Records from pending or aborted transactions must be invisible to readers to maintain transaction isolation.

**Why handle tombstones specially?** When deletes are implemented via tombstones, finding a committed tombstone record means the key has been deleted, so we should return not found instead of continuing to search.

## 5. Additional Correctness Considerations

### 5.1 Clock Monotonicity

**Problem:** If the system clock jumps backward, transaction ordering becomes inconsistent, breaking correctness guarantees.

**Solution:** Use a hybrid logical clock or monotonic clock source for transaction timestamps:

```go
type Clock interface {
    Now() time.Time
    // Ensures time never goes backward
    MonotonicNow() int64 
}
```

**Why matters:** Transaction correctness depends on the temporal ordering of transactions. Non-monotonic clocks can create impossible orderings.

### 5.2 Zombie Transactions

**Problem:** Transactions that start but never commit/abort will accumulate and leak memory.

**Solution:** Implement transaction timeout mechanism:

```go
func (tm *TxnManager) PruneOldTransactions() {
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    threshold := tm.currentTs().Add(-tm.maxTxnLifetime)
    for txID, tx := range tm.transactions {
        if tx.Status == "pending" && tx.LastActive.Before(threshold) {
            tx.Status = "aborted"
            // Mark for eventual cleanup
        }
    }
}
```

**Why matters:** Without cleanup, the system would eventually exhaust memory as abandoned transactions accumulate.

### 5.3 Compaction Considerations

**Problem:** Compaction must preserve transactional visibility semantics.

**Solution:** Modify compaction logic to:
1. Skip records from aborted transactions entirely
2. Strip TxID from records with committed transactions (optional optimization)
3. Preserve TxID for pending transactions

**Why matters:** Compaction must not change the visibility rules for transactional records.

### 5.4 Restart Semantics

**Problem:** In-memory transaction state is lost on process restart.

**Solution:** Define clear semantics for post-restart:
1. All pending transactions at time of restart are considered aborted
2. Records with TxIDs not in the transaction manager are treated as from aborted transactions
3. Client must be prepared to retry transactions after restart

**Why matters:** Clients need clear expectations about transaction durability guarantees.

### 5.5 Transaction ID Collisions

**Problem:** If transaction IDs aren't globally unique, collisions could occur after restart.

**Solution:** Use combination of timestamp, process ID, and sequence number for transaction IDs:

```go
func generateTxID() string {
    return fmt.Sprintf("tx-%d-%d-%d", time.Now().UnixNano(), os.Getpid(), atomic.AddUint64(&txCounter, 1))
}
```

**Why matters:** ID collisions could lead to incorrectly associating records with the wrong transaction.

## 6. Implementation Strategy

1. **Add Transaction Manager**
   - Implement TxnManager with in-memory state tracking
   - Add timeout mechanism for zombie transactions
   - Integrate monotonic clock source

2. **Extend Record Type**
   - Add TxID field to Record struct
   - Update serialization/deserialization

3. **Modify Write Path**
   - Implement explicit transaction methods
   - Modify Put to use implicit transactions
   - Add write set tracking

4. **Update Read Path**
   - Modify Get to filter based on transaction visibility
   - Update index and filter usage to account for transactions

5. **Update Compaction**
   - Modify compaction to handle transactional records appropriately
   - Implement cleanup of transaction metadata

## 7. Testing Strategy

### 7.1 Unit Tests

```go
func TestTransactionBasics(t *testing.T)
func TestTransactionConflictDetection(t *testing.T)
func TestImplicitTransactions(t *testing.T)
func TestTransactionVisibility(t *testing.T)
func TestTransactionTimeout(t *testing.T)
func TestConcurrentTransactions(t *testing.T)
```

**Why extensive unit testing?** Transactional semantics have subtle edge cases that must be thoroughly verified.

Unit tests should cover:
- Transaction state transitions
- Conflict detection correctness
- Read visibility rules
- Transaction timeouts
- Interaction between transactional and non-transactional writes
- Clock anomalies

### 7.2 Integration Tests

```go
func TestTransactionWithCompaction(t *testing.T)
func TestTransactionWithFlush(t *testing.T)
func TestTransactionRecovery(t *testing.T)
```

**Why integration testing?** Transactions interact with multiple system components; integration tests verify these interactions.

Integration tests should verify:
- Transaction behavior across memtable flushes
- Transaction visibility after compaction
- System behavior after restart with pending transactions

### 7.3 Chaos Testing

Extend existing chaos tests to include transactions:

```go
type txOperation struct {
    txID    string
    key     string
    value   []byte
}

func (o txOperation) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error
```

**Why chaos testing?** Transactional systems have complex interactions that are difficult to predict. Chaos testing helps find edge cases.

Chaos test additions:
- Randomly start, commit, and abort transactions
- Mix transactional and non-transactional writes
- Force timeouts and retries
- Trigger compactions during active transactions
- Restart process with pending transactions

**Why this approach?** The existing chaos testing framework has proven effective at finding concurrency issues; extending it to transactions leverages this capability.

## 8. Future Extensions

### 8.1 Durable Transaction Metadata

```go
type DurableTxnManager struct {
    mongo   *mongo.Collection
    cache   map[string]*Transaction
    clock   Clock
}
```

**Why eventually needed?** To support multi-writer scenarios and maintain transaction state across restarts.

### 8.2 Delete Operations

```go
func (b *Blobby) DeleteInTransaction(ctx context.Context, txID string, key string) (string, error)
```

**Why implement as tombstones?** Maintains append-only architecture while allowing logical deletes.

### 8.3 Read-Write Conflict Detection

```go
type Transaction struct {
    // existing fields
    ReadSet     map[string]time.Time
}
```

**Why track read sets?** Enables detection of phantoms and stronger isolation levels like serializable.

### 8.4 Multi-Writer Support

**Why challenging?** Requires distributed consensus on transaction outcomes and proper handling of concurrent operations.

## 9. Assumptions and Limitations

- **Single writer**: Initial design assumes single-writer system
- **Memory-bound**: Transaction state limited by available memory
- **Restart behavior**: All pending transactions aborted on restart
- **No distributed transactions**: No coordination across multiple Blobby instances
- **Write conflicts only**: Read-write conflicts not detected
- **Implicit timeouts**: Long-running transactions may be timed out

**Why document limitations?** Sets clear expectations for system users and highlights future improvement areas.
