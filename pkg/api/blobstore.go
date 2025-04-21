package api

import (
	"context"
	"io"
)

// BlobStore defines low-level blob storage operations
type BlobStore interface {
	// Put stores a blob with the given key
	Put(ctx context.Context, key string, data io.Reader) error

	// Get retrieves a blob with the given key
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// GetRange retrieves a byte range of a blob (inclusive)
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)

	// Delete removes a blob
	Delete(ctx context.Context, key string) error
}
