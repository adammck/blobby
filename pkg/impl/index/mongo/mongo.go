package mongo

import (
	"context"
	"fmt"

	"github.com/adammck/blobby/pkg/api"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	collection = "indices"
	kId        = "_id"
	kKey       = "key"
	kOffset    = "offset"
	kEntries   = "entries"
)

type IndexStore struct {
	db *mongo.Database
}

func New(db *mongo.Database) *IndexStore {
	return &IndexStore{
		db: db,
	}
}

func (s *IndexStore) Init(ctx context.Context) error {
	err := s.db.CreateCollection(ctx, collection)
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}

	return nil
}

func (s *IndexStore) Put(ctx context.Context, filename string, entries api.Index) error {
	bsonEntries := make([]bson.M, len(entries))
	for i, entry := range entries {
		bsonEntries[i] = bson.M{
			kKey:    entry.Key,
			kOffset: entry.Offset,
		}
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.db.Collection(collection).UpdateOne(
		ctx,
		bson.M{kId: filename},
		bson.M{"$set": bson.M{kEntries: bsonEntries}},
		opts,
	)
	if err != nil {
		return fmt.Errorf("UpdateOne: %w", err)
	}

	return nil
}

func (s *IndexStore) Get(ctx context.Context, filename string) (api.Index, error) {
	var result bson.M

	err := s.db.Collection(collection).FindOne(ctx, bson.M{kId: filename}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, &api.IndexNotFound{
				Filename: filename,
			}
		}
	}

	entriesRaw, ok := result[kEntries]
	if !ok {
		return nil, fmt.Errorf("entries field not found")
	}

	entriesArr, ok := entriesRaw.(bson.A)
	if !ok {
		return nil, fmt.Errorf("entries field is not a BSON array")
	}

	var key string
	var offset int64
	entries := make([]api.IndexEntry, len(entriesArr))
	for i, entryRaw := range entriesArr {
		entry, ok := entryRaw.(bson.M)
		if !ok {
			return nil, fmt.Errorf("entry is not a BSON map")
		}

		// TODO: handle wrong types.
		for k, v := range entry {
			switch k {
			case kKey:
				key, _ = v.(string)
			case kOffset:
				offset, _ = v.(int64)
			}
		}

		entries[i] = api.IndexEntry{
			Key:    key,
			Offset: offset,
		}
	}

	return entries, nil
}

func (s *IndexStore) Delete(ctx context.Context, filename string) error {
	_, err := s.db.Collection(collection).DeleteOne(ctx, bson.M{"_id": filename})
	if err != nil {
		return fmt.Errorf("DeleteOne: %w", err)
	}

	return nil
}
