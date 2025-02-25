package memtable

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultDB               = "blobby"
	metaCollectionName      = "meta"
	memtablesCollectionName = "memtables"
	metaActiveMemtableDocID = "active_memtable"

	// How long to sleep before retrying a failed insert. This must be more than
	// a millisecond, because that's the resolution of BSON timestamps which we
	// use for ordering.
	retrySleep  = 1 * time.Millisecond
	retryJitter = 100 * time.Microsecond // 0.1ms
)

type Memtable struct {
	mongoURL string
	mongo    *mongo.Database
	clock    clockwork.Clock
}

func New(mongoURL string, clock clockwork.Clock) *Memtable {
	return &Memtable{
		mongoURL: mongoURL,
		clock:    clock,
	}
}

func (mt *Memtable) Get(ctx context.Context, key string) (*types.Record, string, error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("GetMongo: %w", err)
	}

	// Get all memtables sorted by creation time (newest first)
	cursor, err := db.Collection(memtablesCollectionName).Find(
		ctx,
		bson.M{},
		options.Find().SetSort(bson.D{{Key: "created", Value: -1}}),
	)
	if err != nil {
		return nil, "", fmt.Errorf("finding memtables: %w", err)
	}
	defer cursor.Close(ctx)

	var memtables []struct {
		ID string `bson:"_id"`
	}
	if err := cursor.All(ctx, &memtables); err != nil {
		return nil, "", fmt.Errorf("reading memtables: %w", err)
	}

	// Try to find the key in each memtable, starting with newest
	for _, memtable := range memtables {
		rec, err := mt.innerGetOneCollection(ctx, db, memtable.ID, key)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, "", fmt.Errorf("innerGetOneCollection(%s): %w", memtable.ID, err)
		}

		if rec != nil {
			return rec, memtable.ID, nil
		}
	}

	// Not found in any memtable
	return nil, "", &NotFound{key}
}

func (mt *Memtable) innerGetOneCollection(ctx context.Context, db *mongo.Database, coll, key string) (*types.Record, error) {
	res := db.Collection(coll).FindOne(ctx, bson.M{"key": key}, options.FindOne().SetSort(bson.M{"ts": -1}))

	b, err := res.Raw()
	if err != nil {
		return nil, err
	}

	var rec types.Record
	err = bson.Unmarshal(b, &rec)
	if err != nil {
		return nil, fmt.Errorf("error decoding record: %w", err)
	}

	return &rec, nil
}

func (mt *Memtable) Put(ctx context.Context, key string, value []byte) (string, error) {
	c, err := mt.activeCollection(ctx)
	if err != nil {
		return "", err
	}

	for {
		_, err = c.InsertOne(ctx, &types.Record{
			Key:       key,
			Timestamp: mt.clock.Now(),
			Document:  value,
		})
		if err == nil {
			break
		}

		// sleep and retry to get a new timestamp
		if mongo.IsDuplicateKeyError(err) {
			jitter := time.Duration(rand.Int63n(retryJitter.Nanoseconds()))
			mt.clock.Sleep(retrySleep + jitter)
			continue
		}

		return "", err
	}

	return c.Name(), nil
}

func (mt *Memtable) Ping(ctx context.Context) error {
	_, err := mt.GetMongo(ctx)
	return err
}

func (mt *Memtable) Init(ctx context.Context) error {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return fmt.Errorf("GetMongo: %w", err)
	}

	// Create meta collection for active memtable tracking
	err = db.CreateCollection(ctx, metaCollectionName)
	if err != nil {
		return fmt.Errorf("CreateCollection(meta): %w", err)
	}

	// Create memtables collection for tracking all memtables
	err = db.CreateCollection(ctx, memtablesCollectionName)
	if err != nil {
		return fmt.Errorf("CreateCollection(memtables): %w", err)
	}

	// Create initial memtable
	handle, err := mt.createNewMemtable(ctx, db)
	if err != nil {
		return fmt.Errorf("createNewMemtable: %w", err)
	}

	// Set it as active
	coll := db.Collection(metaCollectionName)
	_, err = coll.InsertOne(ctx, bson.M{
		"_id":   metaActiveMemtableDocID,
		"value": handle.Name(),
	})
	if err != nil {
		return fmt.Errorf("InsertOne: %w", err)
	}

	return nil
}

func (mt *Memtable) GetMongo(ctx context.Context) (*mongo.Database, error) {
	if mt.mongo != nil {
		return mt.mongo, nil
	}

	m, err := connectToMongo(ctx, mt.mongoURL)
	if err != nil {
		return nil, err
	}

	mt.mongo = m
	return m, nil
}

func connectToMongo(ctx context.Context, url string) (*mongo.Database, error) {
	opt := options.Client().ApplyURI(url).SetTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client.Database(defaultDB), nil
}

func (mt *Memtable) activeCollection(ctx context.Context) (*mongo.Collection, error) {
	m, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, err
	}

	cn, err := activeCollectionName(ctx, m)
	if err != nil {
		return nil, err
	}

	return m.Collection(cn), nil
}

func (mt *Memtable) ActiveCollectionName(ctx context.Context) (string, error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return "", fmt.Errorf("GetMongo: %w", err)
	}

	return activeCollectionName(ctx, db)
}

func activeCollectionName(ctx context.Context, db *mongo.Database) (string, error) {
	res := db.Collection(metaCollectionName).FindOne(ctx, bson.M{"_id": metaActiveMemtableDocID})

	var doc bson.M
	err := res.Decode(&doc)
	if err != nil {
		return "", fmt.Errorf("error decoding active memtable doc: %w", err)
	}

	val, ok := doc["value"]
	if !ok {
		return "", fmt.Errorf("no value key in active memtable doc: %#v", doc)
	}

	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("value in active memtable doc was not string, was: %T", val)
	}

	return s, nil
}

// Creates a new memtable with unique name and returns a handle to it
func (mt *Memtable) createNewMemtable(ctx context.Context, db *mongo.Database) (*Handle, error) {
	name := fmt.Sprintf("mt_%d", mt.clock.Now().UTC().UnixNano())

	// Create the collection
	handle := NewHandle(db, name)
	if err := handle.Create(ctx); err != nil {
		return nil, fmt.Errorf("handle.Create: %w", err)
	}

	// Add to memtables tracking collection
	_, err := db.Collection(memtablesCollectionName).InsertOne(
		ctx,
		bson.M{
			"_id":     name,
			"created": mt.clock.Now(),
			"status":  "active",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error tracking memtable: %w", err)
	}

	return handle, nil
}

// Swap creates a new memtable, marks it as active, and returns both the previously
// active memtable and the new active memtable
func (mt *Memtable) Swap(ctx context.Context) (hPrev *Handle, hNext *Handle, err error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("GetMongo: %w", err)
	}

	// Get current active memtable name
	activeName, err := activeCollectionName(ctx, db)
	if err != nil {
		return nil, nil, fmt.Errorf("activeCollectionName: %w", err)
	}

	// Create a new memtable
	hNext, err = mt.createNewMemtable(ctx, db)
	if err != nil {
		return nil, nil, fmt.Errorf("createNewMemtable: %w", err)
	}

	// Update the active memtable reference
	_, err = db.Collection(metaCollectionName).UpdateOne(
		ctx,
		bson.M{"_id": metaActiveMemtableDocID},
		bson.M{"$set": bson.M{"value": hNext.Name()}},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error updating active memtable: %w", err)
	}

	// Mark the previous memtable as "flushing"
	_, err = db.Collection(memtablesCollectionName).UpdateOne(
		ctx,
		bson.M{"_id": activeName},
		bson.M{"$set": bson.M{"status": "flushing"}},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error updating memtable status: %w", err)
	}

	// Return both handles
	hPrev = NewHandle(db, activeName)
	return hPrev, hNext, nil
}

// DropMemtable completely removes a memtable after it has been flushed
func (mt *Memtable) DropMemtable(ctx context.Context, name string) error {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return fmt.Errorf("GetMongo: %w", err)
	}

	// Drop the collection
	err = db.Collection(name).Drop(ctx)
	if err != nil {
		return fmt.Errorf("Drop: %w", err)
	}

	// Remove from tracking collection
	_, err = db.Collection(memtablesCollectionName).DeleteOne(ctx, bson.M{"_id": name})
	if err != nil {
		return fmt.Errorf("error removing memtable tracking: %w", err)
	}

	return nil
}
