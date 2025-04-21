package api

import (
	"context"
	"fmt"
)

type FilterDecoded interface {
	Contains(key string) bool
	Marshal() (*FilterEncoded, error)
}

// FilterEncoded encapsulates a serialized set-membership filter, e.g. a bloom
// filter, and the type information needed to deserialize it. These are read
// from and written to a FilterStore.
type FilterEncoded struct {
	// Type identifies the algorithm (e.g., "xor", "ribbon", "bloom").
	Type string `bson:"type"`

	// Data contains the serialized filter.
	Data []byte `bson:"data"`
}

// FilterStore defines the interface for storing/retrieving filters
type FilterStore interface {
	// Put stores a filter for the given sstable filename
	Put(ctx context.Context, filename string, filter *FilterEncoded) error

	// Get retrieves the filter for the given sstable filename
	Get(ctx context.Context, filename string) (*FilterEncoded, error)

	// Delete deletes the filter for the given sstable filename
	Delete(ctx context.Context, filename string) error
}

// FilterNotFound is returned when the filter doesn't exist
type FilterNotFound struct {
	Filename string
}

func (e *FilterNotFound) Error() string {
	return fmt.Sprintf("filter not found: %s", e.Filename)
}

func (e *FilterNotFound) Is(err error) bool {
	_, ok := err.(*FilterNotFound)
	return ok
}
