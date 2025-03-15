package api

import (
	"context"
	"fmt"
)

// IndexEntry stores the byte offset at which a key is found in an sstable.
type IndexEntry struct {
	Key    string
	Offset int64
}

// Index is a slice of IndexEntry. Each sstable should have one of these.
type Index []IndexEntry

type IndexStore interface {

	// Put stores the index for the given sstable filename.
	// If the index already exists, it will be overwritten.
	Put(ctx context.Context, filename string, entries Index) error

	// Get retrieves the index for the given sstable filename.
	// If the index does not exist, IndexNotFound is returned.
	Get(ctx context.Context, filename string) (Index, error)

	// Delete deletes the index for the given sstable filename.
	// If the index does not exist, the call is a no-op.
	Delete(ctx context.Context, filename string) error
}

// IndexNotFound is returned by IndexStore imlementations when the given sstable
// filename is not found.
type IndexNotFound struct {
	Filename string
}

func (e *IndexNotFound) Error() string {
	return fmt.Sprintf("index not found: %s", e.Filename)
}

func (e *IndexNotFound) Is(err error) bool {
	_, ok := err.(*IndexNotFound)
	return ok
}
