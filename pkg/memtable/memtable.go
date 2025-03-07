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
	defaultDB           = "blobby"
	metaCollection      = "meta"
	memtablesCollection = "memtables"
	activeMemtableKey   = "active_memtable"

	// How long to sleep before retrying a failed insert. This must be more than
	// a millisecond, because that's the resolution of BSON timestamps which we
	// use for ordering.
	retrySleep  = 1 * time.Millisecond
	retryJitter = 100 * time.Microsecond // 0.1ms
)

type memtableInfo struct {
	ID      string    `bson:"_id"`
	Created time.Time `bson:"created,omitempty"`
	Status  string    `bson:"status,omitempty"`
}

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

	cur, err := db.Collection(memtablesCollection).Find(
		ctx,
		bson.M{},
		options.Find().SetSort(bson.D{{Key: "created", Value: -1}}))
	if err != nil {
		return nil, "", fmt.Errorf("db.Collection: %w", err)
	}
	defer cur.Close(ctx)

	var memtables []memtableInfo
	if err := cur.All(ctx, &memtables); err != nil {
		return nil, "", fmt.Errorf("cur.All: %w", err)
	}

	// try to find the key in each collection, starting with newest.
	for _, memtable := range memtables {
		rec, err := mt.getOneColl(ctx, db, memtable.ID, key)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, "", fmt.Errorf("innerGetOneCollection(%s): %w", memtable.ID, err)
		}
		if rec != nil {
			return rec, memtable.ID, nil
		}
	}

	// not found in any collection.
	return nil, "", &NotFound{key}
}

func (mt *Memtable) getOneColl(ctx context.Context, db *mongo.Database, coll, key string) (*types.Record, error) {
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

		// sleep and retry to get a new timestamp. it's almost certainly been
		// long enough already, but this makes testing easier.
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

	err = db.CreateCollection(ctx, metaCollection)
	if err != nil {
		return fmt.Errorf("CreateCollection(%s): %w", metaCollection, err)
	}

	err = db.CreateCollection(ctx, memtablesCollection)
	if err != nil {
		return fmt.Errorf("CreateCollection(%s): %w", memtablesCollection, err)
	}

	handle, err := mt.createNext(ctx, db)
	if err != nil {
		return fmt.Errorf("createNewMemtable: %w", err)
	}

	coll := db.Collection(metaCollection)
	_, err = coll.InsertOne(ctx, bson.M{
		"_id":   activeMemtableKey,
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

func activeCollectionName(ctx context.Context, db *mongo.Database) (string, error) {
	res := db.Collection(metaCollection).FindOne(ctx, bson.M{"_id": activeMemtableKey})

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

func (mt *Memtable) createNext(ctx context.Context, db *mongo.Database) (*Handle, error) {
	name := fmt.Sprintf("mt_%d", mt.clock.Now().UTC().UnixNano())

	handle := NewHandle(db, name)
	if err := handle.Create(ctx); err != nil {
		return nil, fmt.Errorf("handle.Create: %w", err)
	}

	_, err := db.Collection(memtablesCollection).InsertOne(ctx, memtableInfo{
		ID:      name,
		Created: mt.clock.Now(),
		Status:  "active",
	})
	if err != nil {
		return nil, fmt.Errorf("error tracking memtable: %w", err)
	}

	return handle, nil
}

func (mt *Memtable) Rotate(ctx context.Context) (hPrev *Handle, hNext *Handle, err error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("GetMongo: %w", err)
	}

	activeName, err := activeCollectionName(ctx, db)
	if err != nil {
		return nil, nil, fmt.Errorf("activeCollectionName: %w", err)
	}

	hNext, err = mt.createNext(ctx, db)
	if err != nil {
		return nil, nil, fmt.Errorf("createNext: %w", err)
	}

	_, err = db.Collection(metaCollection).UpdateOne(
		ctx,
		bson.M{"_id": activeMemtableKey},
		bson.M{"$set": bson.M{"value": hNext.Name()}},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("UpdateOne: %w", err)
	}

	_, err = db.Collection(memtablesCollection).UpdateOne(
		ctx,
		bson.M{"_id": activeName},
		bson.M{"$set": bson.M{"status": "flushing"}},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("UpdateOne: %w", err)
	}

	hPrev = NewHandle(db, activeName)
	return hPrev, hNext, nil
}

func (mt *Memtable) Drop(ctx context.Context, name string) error {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return fmt.Errorf("GetMongo: %w", err)
	}

	err = db.Collection(name).Drop(ctx)
	if err != nil {
		return fmt.Errorf("Drop: %w", err)
	}

	_, err = db.Collection(memtablesCollection).DeleteOne(ctx, bson.M{"_id": name})
	if err != nil {
		return fmt.Errorf("DeleteOne: %w", err)
	}

	return nil
}
