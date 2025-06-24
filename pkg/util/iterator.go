// Package util provides common utilities for iterator operations
package util

import (
	"context"
	"fmt"
	"io"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/types"
)

// IterateRecords provides a common pattern for iterating over API iterators
// and applying a function to each record. The iterator is automatically closed.
func IterateRecords(ctx context.Context, iter api.Iterator, fn func(*types.Record) error) error {
	defer iter.Close()
	
	for iter.Next(ctx) {
		rec := &types.Record{
			Key:       iter.Key(),
			Document:  iter.Value(),
			Timestamp: iter.Timestamp(),
		}
		
		if err := fn(rec); err != nil {
			return err
		}
	}
	
	return iter.Err()
}

// SendRecords sends records from a reader to a channel with context cancellation support.
// This is a common pattern in compaction operations.
type RecordReader interface {
	Next() (*types.Record, error)
}

func SendRecords(ctx context.Context, reader RecordReader, ch chan<- *types.Record) error {
	for {
		rec, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("RecordReader.Next: %w", err)
		}
		
		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// SendFilteredRecords sends filtered records from a reader to a channel.
// The filter function determines which records to send.
func SendFilteredRecords(ctx context.Context, reader RecordReader, ch chan<- *types.Record, filter func(*types.Record) bool) error {
	for {
		rec, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("RecordReader.Next: %w", err)
		}
		
		if filter(rec) {
			select {
			case ch <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// FindRecord searches for a specific key in a reader, returning the first match.
// Returns nil if the key is not found.
func FindRecord(reader RecordReader, key string) (*types.Record, int, error) {
	scanned := 0
	
	for {
		rec, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				return nil, scanned, nil
			}
			return nil, scanned, fmt.Errorf("RecordReader.Next: %w", err)
		}
		if rec == nil {
			return nil, scanned, nil
		}
		
		scanned++
		
		if rec.Key == key {
			return rec, scanned, nil
		}
	}
}