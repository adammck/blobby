package api

import (
	"context"
	"fmt"
	"time"
)

type NotFound struct {
	Key string
}

func (e *NotFound) Error() string {
	return fmt.Sprintf("not found: %s", e.Key)
}

func (e *NotFound) Is(err error) bool {
	_, ok := err.(*NotFound)
	return ok
}

type GetStats struct {
	Source         string
	BlobsFetched   int
	BlobsSkipped   int
	RecordsScanned int
}

// TODO: Implement PutStats
type PutStats struct {
}

// DeleteStats contains metadata about a delete operation.
type DeleteStats struct {
	Timestamp   time.Time
	Destination string
}

// ScanStats contains metadata about a range scan operation.
// These statistics are populated incrementally during the scan:
// - Source counts (MemtablesRead, SstablesRead, BlobsSkipped) are available immediately after Scan() returns
// - RecordsReturned is updated as the iterator is consumed and only accurate after the scan completes
type ScanStats struct {
	RecordsReturned int // Updated incrementally as iterator is consumed
	BlobsSkipped    int // Set when scan is created (currently unused in Scan, only in Get)
	MemtablesRead   int // Set when scan is created
	SstablesRead    int // Set when scan is created (SSTables actually read)
}

// Iterator provides ordered access to key-value pairs within a range.
type Iterator interface {
	// Next advances the iterator and returns true if a record is available.
	Next(ctx context.Context) bool

	// Key returns the current key. Only valid after Next() returns true.
	Key() string

	// Value returns the current value. Only valid after Next() returns true.
	Value() []byte

	// Timestamp returns the timestamp of the current record.
	Timestamp() time.Time

	// Err returns any error encountered during iteration.
	Err() error

	// Close releases resources associated with the iterator.
	Close() error
}

type FlushStats struct {

	// The URL of the memtable which was flushed.
	FlushedMemtable string

	// The name of the memtable that is now active, after the flush.
	ActiveMemtable string

	// Metadata about the flushed sstable.
	Meta *BlobMeta
}

type CompactionOrder int

const (
	// OldestFirst considers files from oldest to newest, in terms of the
	// creation time, not the timestamps of the records it contains. This is
	// useful when old files are likely to contain data which can be expired.
	OldestFirst CompactionOrder = iota

	// NewestFirst considers files from newest to oldest. This is useful when
	// files are created rapidly, and should be compacted together regularly.
	// Should usually be combined with MinTime and/or MaxSize.
	NewestFirst

	// SmallestFirst considers files from smallest to largest. This is useful
	// when the overhead of having or touching many files is high, and we wish
	// to reduce the number of them.
	SmallestFirst

	// LargestFirst considers files from largest to smallest. Like OldestFirst,
	// this is most useful when looking for data to delete, or when scanning is
	// expensive and we want to repartition files.
	LargestFirst
)

type CompactionOptions struct {
	// Order specifies the order in which files should be considered for
	// compaction. The default is OldestFirst.
	Order CompactionOrder

	// MinTime specifies the minimum Timestamp of record which we want to
	// compact. Files containing only records older than this will be ignored.
	// Note that this does not affect time partioning of the output files.
	MinTime time.Time

	// MaxTime specifies the maximum Timestamp of record which we want to
	// compact. Files containing only records newer than this will be ignored.
	// Note that this does not affect time partioning of the output files.
	MaxTime time.Time

	// MinInputSize specifies the minimum total number of bytes which we will
	// compact. This is to avoid scheduling tiny compactions which are a waste
	// of time.
	MinInputSize int64

	// MaxInputSize specifies the maximum total number of bytes which we will
	// compact. This is to avoid scheduling huge compactions which take forever
	// or never complete.
	MaxInputSize int64

	// MinFiles specifies the minimum number of files which we will compact at
	// once. I'm not sure why this is here. Prefer MinInputSize.
	MinFiles int

	// MaxFiles specifies the maximum number of files which we will compact at
	// once. This is mostly to avoid shuffling too much metadata around.
	MaxFiles int

	// only compact a subset of the keyspace?
	//MinKey string
	//MaxKey string
}

type CompactionStats struct {
	Inputs  []*BlobMeta
	Outputs []*BlobMeta

	// Contains an error if the compaction failed.
	Error error
}

// Blobby is the public interface to Blobby. It's defined here so test doubles
// can implement it too.
type Blobby interface {
	Put(ctx context.Context, key string, value []byte) (string, error)
	Get(ctx context.Context, key string) ([]byte, *GetStats, error)
	Scan(ctx context.Context, start, end string) (Iterator, *ScanStats, error)
	Flush(ctx context.Context) (*FlushStats, error)
	Compact(ctx context.Context, opts CompactionOptions) ([]*CompactionStats, error)
	Delete(ctx context.Context, key string) (*DeleteStats, error)
}
