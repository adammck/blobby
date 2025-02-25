package memtable

import (
	"context"
	"fmt"

	"github.com/adammck/blobby/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Handle struct {
	db   *mongo.Database
	coll *mongo.Collection
}

func NewHandle(db *mongo.Database, name string) *Handle {
	return &Handle{
		db:   db,
		coll: db.Collection(name),
	}
}

// Name returns the name of the collection serving this handle. This may be
// formatted arbitrarily, and should only be used for display.
func (h *Handle) Name() string {
	return h.coll.Name()
}

func (h *Handle) Flush(ctx context.Context, ch chan *types.Record) error {
	cur, err := h.coll.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("Find: %w", err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var rec types.Record
		var err error

		err = cur.Decode(&rec)
		if err != nil {
			return fmt.Errorf("Decode: %w", err)
		}

		ch <- &rec
	}

	close(ch)

	err = cur.Err()
	if err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	return nil
}

// Create initializes the collection for this handle with appropriate indexes
func (h *Handle) Create(ctx context.Context) error {
	err := h.db.CreateCollection(ctx, h.coll.Name())
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}

	_, err = h.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "key", Value: 1},
			{Key: "ts", Value: -1},
		},
		// Unlikely that two writes arrive at the exact same time, but possible,
		// and this keeps them ordered, assuming the clock never goes backwards.
		// (We use the client clock, not $createdAt, so we can inject a fake
		// clock for testing.)
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("CreateIndex: %w", err)
	}

	return nil
}

// Count returns the number of documents in the collection
func (h *Handle) Count(ctx context.Context) (int64, error) {
	return h.coll.CountDocuments(ctx, bson.M{})
}

// IsEmpty returns true if the collection has no documents
func (h *Handle) IsEmpty(ctx context.Context) (bool, error) {
	count, err := h.Count(ctx)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}
