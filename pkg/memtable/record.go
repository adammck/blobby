package memtable

import (
	"context"
	"math/rand"
	"time"

	"github.com/adammck/blobby/pkg/types"
	"go.mongodb.org/mongo-driver/mongo"
)

// PutRecord writes a record with the given transaction ID to the memtable.
// This is similar to Put but takes a pre-constructed record.
func (mt *Memtable) PutRecord(ctx context.Context, record *types.Record) (string, error) {
	c, err := mt.activeCollection(ctx)
	if err != nil {
		return "", err
	}

	for {
		_, err = c.InsertOne(ctx, record)
		if err == nil {
			break
		}

		// Sleep and retry to get a new timestamp. It's almost certainly been
		// long enough already, but this makes testing easier.
		if mongo.IsDuplicateKeyError(err) {
			jitter := time.Duration(rand.Int63n(retryJitter.Nanoseconds()))
			mt.clock.Sleep(retrySleep + jitter)

			// Update the timestamp to avoid conflicts
			record.Timestamp = mt.clock.Now()
			continue
		}

		return "", err
	}

	return c.Name(), nil
}
