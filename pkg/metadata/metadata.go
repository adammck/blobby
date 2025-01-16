package metadata

import (
	"context"
	"fmt"
	"time"

	"github.com/adammck/archive/pkg/sstable"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultDB         = "archive"
	collectionName    = "sstables"
	connectionTimeout = 3 * time.Second
)

type Store struct {
	mongo    *mongo.Database
	mongoURL string
}

func New(mongoURL string) *Store {
	return &Store{
		mongoURL: mongoURL,
	}
}

func (s *Store) getMongo(ctx context.Context) (*mongo.Database, error) {
	if s.mongo != nil {
		return s.mongo, nil
	}

	opt := options.Client().ApplyURI(s.mongoURL).SetTimeout(connectionTimeout)
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	s.mongo = client.Database(defaultDB)
	return s.mongo, nil
}

func (s *Store) Init(ctx context.Context) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	err = db.CreateCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}

	_, err = db.Collection(collectionName).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "min_key", Value: 1},
			{Key: "max_key", Value: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateIndex: %w", err)
	}

	return nil
}

func (s *Store) Insert(ctx context.Context, meta *sstable.Meta) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	_, err = db.Collection(collectionName).InsertOne(ctx, meta)
	if err != nil {
		return fmt.Errorf("InsertOne: %w", err)
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, meta *sstable.Meta) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	// TODO: add an ID to Meta and use that instead of this weird filter.
	result, err := db.Collection(collectionName).DeleteOne(ctx, bson.M{
		"created": meta.Created,
		"min_key": meta.MinKey,
		"max_key": meta.MaxKey,
	})
	if err != nil {
		return fmt.Errorf("DeleteOne: %w", err)
	}

	if result.DeletedCount != 1 {
		return fmt.Errorf("expected to delete 1 record, deleted %d", result.DeletedCount)
	}

	return nil
}

func (s *Store) GetContaining(ctx context.Context, key string) ([]*sstable.Meta, error) {
	db, err := s.getMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getMongo: %w", err)
	}

	cursor, err := db.Collection(collectionName).Find(ctx, bson.M{
		"min_key": bson.M{"$lte": key},
		"max_key": bson.M{"$gte": key},
	}, options.Find().SetSort(bson.D{
		{Key: "max_time", Value: -1},
		{Key: "created", Value: -1}, // tie-breaker
	}))
	if err != nil {
		return nil, fmt.Errorf("Find: %w", err)
	}
	defer cursor.Close(ctx)

	var metas []*sstable.Meta
	if err := cursor.All(ctx, &metas); err != nil {
		return nil, fmt.Errorf("cursor.All: %w", err)
	}

	return metas, nil
}

func (s *Store) GetAllMetas(ctx context.Context) ([]*sstable.Meta, error) {
	db, err := s.getMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getMongo: %w", err)
	}

	cur, err := db.Collection(collectionName).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{
		{Key: "min_key", Value: 1},
		{Key: "max_time", Value: -1},
	}))
	if err != nil {
		return nil, fmt.Errorf("Find: %w", err)
	}
	defer cur.Close(ctx)

	var metas []*sstable.Meta
	if err := cur.All(ctx, &metas); err != nil {
		return nil, fmt.Errorf("cursor.All: %w", err)
	}

	return metas, nil
}
