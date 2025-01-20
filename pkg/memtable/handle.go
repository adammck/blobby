package memtable

import (
	"context"
	"fmt"

	"github.com/adammck/archive/pkg/types"
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

	// TODO: does this belong at the bottom?
	close(ch)

	err = cur.Err()
	if err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	return nil
}

func (h *Handle) Truncate(ctx context.Context) error {
	err := h.coll.Drop(ctx)
	if err != nil {
		return fmt.Errorf("Drop: %w", err)
	}

	return h.Create(ctx)
}

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
