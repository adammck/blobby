package metadata

import (
	"context"
	"fmt"
	"time"

	"github.com/adammck/blobby/pkg/api"
	sharedmongo "github.com/adammck/blobby/pkg/shared/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultDB          = "blobby"
	sstablesCollection = "sstables"
	connectionTimeout  = 3 * time.Second
	pingTimeout        = 3 * time.Second
)

type Store struct {
	mongoClient *sharedmongo.Client
}

func New(mongoURL string) *Store {
	return &Store{
		mongoClient: sharedmongo.NewClient(mongoURL).WithDirect(true),
	}
}

func (s *Store) getMongo(ctx context.Context) (*mongo.Database, error) {
	return s.mongoClient.GetDB(ctx)
}

func (s *Store) Init(ctx context.Context) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	err = db.CreateCollection(ctx, sstablesCollection)
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}

	_, err = db.Collection(sstablesCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
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

func (s *Store) Insert(ctx context.Context, meta *api.BlobMeta) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	_, err = db.Collection(sstablesCollection).InsertOne(ctx, meta)
	if err != nil {
		return fmt.Errorf("InsertOne: %w", err)
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, meta *api.BlobMeta) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	// TODO: add an ID to Meta and use that instead of this weird filter.
	result, err := db.Collection(sstablesCollection).DeleteOne(ctx, bson.M{
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

// AtomicSwap atomically replaces old metadata entries with a new one.
// This is used for compaction to ensure metadata consistency.
func (s *Store) AtomicSwap(ctx context.Context, newMeta *api.BlobMeta, oldMetas []*api.BlobMeta) error {
	db, err := s.getMongo(ctx)
	if err != nil {
		return fmt.Errorf("getMongo: %w", err)
	}

	// Use MongoDB transaction for atomic metadata swap
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		coll := db.Collection(sstablesCollection)

		// Insert new metadata
		_, err := coll.InsertOne(sessCtx, newMeta)
		if err != nil {
			return nil, fmt.Errorf("insert new meta: %w", err)
		}

		// Delete old metadata entries
		for i, oldMeta := range oldMetas {
			result, err := coll.DeleteOne(sessCtx, bson.M{
				"created": oldMeta.Created,
				"min_key": oldMeta.MinKey,
				"max_key": oldMeta.MaxKey,
			})
			if err != nil {
				return nil, fmt.Errorf("delete old meta %d: %w", i, err)
			}
			if result.DeletedCount != 1 {
				return nil, fmt.Errorf("expected to delete 1 record for meta %d, deleted %d", i, result.DeletedCount)
			}
		}

		return nil, nil
	}

	// Execute transaction with retries
	session, err := db.Client().StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

func (s *Store) GetContaining(ctx context.Context, key string) ([]*api.BlobMeta, error) {
	db, err := s.getMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getMongo: %w", err)
	}

	cursor, err := db.Collection(sstablesCollection).Find(ctx, bson.M{
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

	var metas []*api.BlobMeta
	if err := cursor.All(ctx, &metas); err != nil {
		return nil, fmt.Errorf("cursor.All: %w", err)
	}

	return metas, nil
}

func (s *Store) GetAllMetas(ctx context.Context) ([]*api.BlobMeta, error) {
	db, err := s.getMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getMongo: %w", err)
	}

	cur, err := db.Collection(sstablesCollection).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{
		{Key: "min_key", Value: 1},
		{Key: "max_time", Value: -1},
	}))
	if err != nil {
		return nil, fmt.Errorf("Find: %w", err)
	}
	defer cur.Close(ctx)

	var metas []*api.BlobMeta
	if err := cur.All(ctx, &metas); err != nil {
		return nil, fmt.Errorf("cursor.All: %w", err)
	}

	return metas, nil
}
