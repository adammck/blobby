# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Testing
- `bin/test.sh` - Run all tests across the project
- `bin/chaostest.sh` - Run chaos testing with concurrent operations and failures
- `go test ./pkg/...` - Run tests for specific packages
- `go test -run TestSpecific ./pkg/blobby` - Run specific test by name

### Building and Running
- `go build -o blobby ./cmd/archive` - Build the CLI binary
- `./blobby init` - Initialize datastore connections
- `./blobby put` - Write records to memtable (reads JSON from stdin)
- `./blobby get <key>` - Retrieve a record by key
- `./blobby flush` - Flush active memtable to SSTable in blob storage
- `./blobby compact` - Compact SSTables with various options

### Environment Variables
Required for operation:
- `MONGO_URL` - MongoDB connection string for memtables and metadata
- `S3_BUCKET` - S3 bucket name for SSTable storage

Optional:
- `BLOBBY_RUN_CHAOS=1` - Enable chaos testing mode

## Architecture

### Core Components

**Blobby** (`pkg/blobby/archive.go`) is the main orchestrator that coordinates:
- **Memtable** (`pkg/memtable/`) - MongoDB-backed write buffer with timestamped collections
- **SSTable Manager** (`pkg/sstable/`) - Manages immutable sorted files in blob storage
- **Compactor** (`pkg/compactor/`) - Merges SSTables with configurable strategies
- **Index Store** - Caches SSTable key→byte offset mappings for efficient seeks
- **Filter Store** - XOR filters to avoid reading SSTables that don't contain keys

### Data Flow

1. **Writes**: Records go to active memtable (timestamped MongoDB collection)
2. **Rotation**: Create new timestamped collection, atomically update active pointer
3. **Flush**: Memtable → SSTable in blob storage, then drop flushed collection
4. **Reads**: Check all memtables (newest first), then SSTables (using filters + indexes)
5. **Compaction**: Merge SSTables based on size, age, or key overlap strategies

### Key Interfaces (`pkg/api/`)

- `Blobby` - Main interface (Put, Get, Delete, Flush, Compact)
- `BlobStore` - Abstract blob storage (S3 implementation in `pkg/impl/blobstore/s3/`)
- `IndexStore` - SSTable key index storage (MongoDB implementation)
- `FilterStore` - Bloom/XOR filter storage for efficient negative lookups

### Record Format

Records (`pkg/types/types.go`) use BSON encoding with:
- `Key` - Primary key string
- `Timestamp` - Write timestamp for MVCC (auto-assigned on insert)
- `Document` - Arbitrary byte payload
- `Tombstone` - Boolean flag for deletions

### Implementation Details

- **Timestamped Memtables**: Uses `mt_{timestamp}` MongoDB collections with atomic active pointer swapping
- **LSM-Tree**: Write-optimized structure with background compaction
- **Filtering**: XOR filters (`pkg/filter/xor/`) minimize unnecessary SSTable reads  
- **Concurrency**: All operations are context-aware and handle cancellation
- **Timestamp Conflicts**: Retries with jitter if BSON timestamp collisions occur
- **Chaos Testing**: Simulates failures during concurrent operations

### Package Organization

- `pkg/api/` - Interfaces only (no pkg imports to avoid cycles)
- `pkg/impl/` - Concrete implementations of interfaces
- `pkg/blobby/` - Main logic and orchestration
- `pkg/types/` - Core data structures and serialization
- `cmd/archive/` - CLI interface