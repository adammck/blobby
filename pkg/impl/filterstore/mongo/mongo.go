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
	collection = "filters"
	kId        = "_id"
)

type FilterStore struct {
	db *mongo.Database
}

func New(db *mongo.Database) *FilterStore {
	return &FilterStore{
		db: db,
	}
}

func (s *FilterStore) Init(ctx context.Context) error {
	err := s.db.CreateCollection(ctx, collection)
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}
	return nil
}

func (s *FilterStore) Put(ctx context.Context, filename string, filter api.FilterInfo) error {
	opts := options.Update().SetUpsert(true)
	_, err := s.db.Collection(collection).UpdateOne(
		ctx,
		bson.M{kId: filename},
		bson.M{"$set": bson.M{
			"type":    filter.Type,
			"version": filter.Version,
			"data":    filter.Data,
		}},
		opts,
	)
	if err != nil {
		return fmt.Errorf("UpdateOne: %w", err)
	}
	return nil
}

func (s *FilterStore) Get(ctx context.Context, filename string) (api.FilterInfo, error) {
	var result struct {
		Type    string `bson:"type"`
		Version string `bson:"version"`
		Data    []byte `bson:"data"`
	}

	err := s.db.Collection(collection).FindOne(ctx, bson.M{kId: filename}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return api.FilterInfo{}, &api.FilterNotFound{Filename: filename}
		}
		return api.FilterInfo{}, fmt.Errorf("FindOne: %w", err)
	}

	return api.FilterInfo{
		Type:    result.Type,
		Version: result.Version,
		Data:    result.Data,
	}, nil
}

func (s *FilterStore) Delete(ctx context.Context, filename string) error {
	_, err := s.db.Collection(collection).DeleteOne(ctx, bson.M{kId: filename})
	if err != nil {
		return fmt.Errorf("DeleteOne: %w", err)
	}
	return nil
}
