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
	metaActiveMemtableDocID = "active_memtable"
	blueMemtableName        = "blue"
	greenMemtableName       = "green"

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

	sess, err := db.Client().StartSession()
	if err != nil {
		return nil, "", err
	}
	defer sess.EndSession(ctx)

	var active string
	var recBlue, recGreen *types.Record

	err = mongo.WithSession(ctx, sess, func(sctx mongo.SessionContext) error {
		err := sess.StartTransaction()
		if err != nil {
			return err
		}

		active, recBlue, recGreen, err = mt.innerGet(sctx, db, key)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, "", fmt.Errorf("mongo.WithSession: %w", err)
	}

	var prio [2]string
	switch active {
	case blueMemtableName:
		prio = [2]string{blueMemtableName, greenMemtableName}
	case greenMemtableName:
		prio = [2]string{greenMemtableName, blueMemtableName}
	default:
		return nil, "", fmt.Errorf("invalid active collection: %s", active)
	}

	recs := map[string]*types.Record{
		blueMemtableName:  recBlue,
		greenMemtableName: recGreen,
	}

	// iterate the collections in priority order. the current one first, and the
	// inactive one after that, to capture anything which was written before the
	// last swap, but not yet flushed to s3.
	for _, name := range prio {
		r, ok := recs[name]
		if ok && r != nil {
			return r, name, err
		}
	}

	// not found in any collection.
	return nil, "", &NotFound{key}
}

func (mt *Memtable) innerGet(ctx mongo.SessionContext, db *mongo.Database, key string) (string, *types.Record, *types.Record, error) {
	active, err := mt.activeCollectionName(ctx, db)
	if err != nil {
		return "", nil, nil, fmt.Errorf("activeCollectionName: %w", err)
	}

	recBlue, errBlue := mt.innerGetOneCollection(ctx, db, blueMemtableName, key)
	if errBlue != nil && errBlue != mongo.ErrNoDocuments {
		return "", nil, nil, fmt.Errorf("innerGetOneCollection(blue): %w", err)
	}

	recGreen, err := mt.innerGetOneCollection(ctx, db, greenMemtableName, key)
	if errBlue != nil && errBlue != mongo.ErrNoDocuments {
		return "", nil, nil, fmt.Errorf("innerGetOneCollection(green): %w", err)
	}

	return active, recBlue, recGreen, nil
}

func (mt *Memtable) innerGetOneCollection(ctx mongo.SessionContext, db *mongo.Database, coll, key string) (*types.Record, error) {
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

	err = db.CreateCollection(ctx, metaCollectionName)
	if err != nil {
		return fmt.Errorf("CreateCollection: %w", err)
	}

	coll := db.Collection(metaCollectionName)
	_, err = coll.InsertOne(ctx, bson.M{
		"_id":   metaActiveMemtableDocID,
		"value": blueMemtableName,
	})
	if err != nil {
		return fmt.Errorf("InsertOne: %w", err)
	}

	blue := NewHandle(db, blueMemtableName)
	if err := blue.Create(ctx); err != nil {
		return err
	}

	green := NewHandle(db, greenMemtableName)
	if err := green.Create(ctx); err != nil {
		return err
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

	cn, err := mt.activeCollectionName(ctx, m)
	if err != nil {
		return nil, err
	}

	return m.Collection(cn), nil
}

func (mt *Memtable) activeCollectionName(ctx context.Context, db *mongo.Database) (string, error) {
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

// Swap updates the active collection with the inactive collection, and returns
// them both.
func (mt *Memtable) Swap(ctx context.Context) (hPrev *Handle, hNext *Handle, err error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return
	}

	nPrev, err := mt.activeCollectionName(ctx, db)
	if err != nil {
		return
	}

	nNext := blueMemtableName
	if nPrev == blueMemtableName {
		nNext = greenMemtableName
	}

	// check that the new collection is empty. this is not safe, because it's
	// not transactional. but we're going to change this soon, to use new
	// collections rather than flipping between two, so it's fine.
	n, err := db.Collection(nNext).CountDocuments(ctx, bson.D{})
	if err != nil {
		err = fmt.Errorf("CountDocuments(%s): %w", nNext, err)
		return
	}
	if n > 0 {
		err = fmt.Errorf("want to activate %s, but is not empty", nNext)
		return
	}

	_, err = db.Collection(metaCollectionName).UpdateOne(
		ctx,
		bson.M{"_id": metaActiveMemtableDocID},
		bson.M{"$set": bson.M{"value": nNext}},
	)
	if err != nil {
		err = fmt.Errorf("error updating active memtable: %w", err)
		return
	}

	hPrev = NewHandle(db, nPrev)
	hNext = NewHandle(db, nNext)
	return
}
