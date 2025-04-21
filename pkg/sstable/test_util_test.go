package sstable

import (
	"testing"

	"github.com/adammck/blobby/pkg/types"
)

// sstableToMap reads an entire SSTable and returns it as a map. This is useful for
// tests, but should never be used in non-test code. If any error occurs, the
// given test fails.
func sstableToMap(t testing.TB, r *Reader) map[string]*types.Record {
	recs := map[string]*types.Record{}

	for {
		rec, err := r.Next()
		if err != nil {
			t.Fatalf("sstable.Map: %v", err)
			return nil
		}

		// end of file
		if rec == nil {
			break
		}

		recs[rec.Key] = rec
	}

	return recs
}
