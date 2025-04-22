package api

import (
	"context"
	"io"
)

// BlobStore defines the interface for storing/retrieving blobs (e.g. sstables),
// which are large (100s of MiBs) immutable files. S3 is the original/canonical
// implementation.
type BlobStore interface {

	// Put stores a blob with the given key.
	Put(ctx context.Context, key string, data io.Reader) error

	// GetRange retrieves a byte range of the blob with the given key. The start
	// offset must be given, but the end may be zero to read to EOF. The caller
	// MUST close the reader to avoid leaking resources.
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)

	// Get retrieves the entire blob with the given key. This should be avoided
	// in favor of GetRange where possible. The caller MUST close the reader to
	// avoid leaking resources.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes the blob with the given key.
	Delete(ctx context.Context, key string) error
}
