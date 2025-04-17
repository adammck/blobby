package api

import (
	"context"
	"time"
)

type GetStats struct {
	Source         string
	BlobsFetched   int
	BlobsSkipped   int
	RecordsScanned int
}

// TODO: Implement PutStats
type PutStats struct {
}

type FlushStats struct {

	// The URL of the memtable which was flushed.
	FlushedMemtable string

	// The name of the memtable that is now active, after the flush.
	ActiveMemtable string

	// The key of the flushed sstable in the blobstore.
	BlobName string

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
	Flush(ctx context.Context) (*FlushStats, error)
	Compact(ctx context.Context, opts CompactionOptions) ([]*CompactionStats, error)
}
