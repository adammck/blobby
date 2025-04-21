package api

import (
	"context"
	"errors"
	"io"

	"github.com/adammck/blobby/pkg/types"
)

// ErrNoRecords is returned from Flush when there are no records to write, i.e.
// the channel is empty or already closed.
var ErrNoRecords = errors.New("NoRecords")

// BlobStore defines storage operations for sstable files
type BlobStore interface {
	// GetFull retrieves a complete blob
	GetFull(ctx context.Context, key string) (io.ReadCloser, error)

	// GetPartial retrieves a byte range of a blob
	GetPartial(ctx context.Context, key string, first, last int64) (io.ReadCloser, error)

	// Delete removes a blob
	Delete(ctx context.Context, key string) error

	// Flush writes a new blob from the records channel
	Flush(ctx context.Context, ch <-chan *types.Record) (dest string, count int, meta *BlobMeta, index []IndexEntry, filter Filter, err error)
}
