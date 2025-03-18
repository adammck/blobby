package api

import (
	"context"
	"fmt"
)

type FilterCreator interface {
	Create(keys []string) (FilterInfo, error)
}

// FilterInfo encapsulates a filter with its type information
type FilterInfo struct {
	// Type identifies the filter algorithm (e.g., "xor", "ribbon", "bloom")
	Type string `bson:"type"`

	// Version allows for backward compatibility within a filter type
	Version string `bson:"version"`

	// Data contains the serialized filter
	Data []byte `bson:"data"`
}

// FilterStore defines the interface for storing/retrieving filters
type FilterStore interface {
	// Put stores a filter for the given sstable filename
	Put(ctx context.Context, filename string, filter FilterInfo) error

	// Get retrieves the filter for the given sstable filename
	Get(ctx context.Context, filename string) (FilterInfo, error)

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
