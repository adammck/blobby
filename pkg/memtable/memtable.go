package memtable

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammck/blobby/pkg/api"
	sharedmongo "github.com/adammck/blobby/pkg/shared/mongo"
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
	
	// Size limits to prevent OOM
	defaultMaxMemtableSize = 1024 * 1024 * 1024 // 1GB default - generous limit for tests
	recordOverheadBytes    = 100                 // estimated BSON overhead per record
)

// ErrMemtableFull is returned when a memtable reaches its size limit
type ErrMemtableFull struct {
	CurrentSize int64
	MaxSize     int64
}

func (e *ErrMemtableFull) Error() string {
	return fmt.Sprintf("memtable full: current size %d bytes exceeds max size %d bytes", 
		e.CurrentSize, e.MaxSize)
}

type memtableInfo struct {
	ID      string    `bson:"_id"`
	Created time.Time `bson:"created,omitempty"`
	Status  string    `bson:"status,omitempty"`
}

type Memtable struct {
	mongoClient *sharedmongo.Client
	clock       clockwork.Clock

	// Handle registry ensures one handle per memtable collection
	// This is critical for reference counting to work correctly across
	// concurrent operations (scans, flushes, compactions)
	handlesMu sync.RWMutex
	handles   map[string]*Handle
	
	// Size tracking for the active memtable to prevent OOM
	activeSizeEstimate int64 // atomic access only
	maxSize           int64
}

func New(mongoURL string, clock clockwork.Clock) *Memtable {
	return &Memtable{
		mongoClient: sharedmongo.NewClient(mongoURL),
		clock:       clock,
		handles:     make(map[string]*Handle),
		maxSize:     defaultMaxMemtableSize,
	}
}

// SetMaxSize configures the maximum size for active memtables
func (mt *Memtable) SetMaxSize(maxSize int64) {
	mt.maxSize = maxSize
}

// GetCurrentSize returns the estimated current size of the active memtable
func (mt *Memtable) GetCurrentSize() int64 {
	return atomic.LoadInt64(&mt.activeSizeEstimate)
}

// GetMaxSize returns the maximum allowed size for memtables
func (mt *Memtable) GetMaxSize() int64 {
	return mt.maxSize
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
	return mt.PutRecord(ctx, &types.Record{
		Key:      key,
		Document: value,
	})
}

// PutRecordStats tracks statistics for put operations
type PutRecordStats struct {
	RetryCount int
}

// PutRecord inserts a pre-constructed record. Use this for tombstones or when
// you need precise control over the record fields. Use Put for normal writes
// and Delete for deletions.
func (mt *Memtable) PutRecord(ctx context.Context, rec *types.Record) (string, error) {
	stats := &PutRecordStats{}
	dest, err := mt.PutRecordWithStats(ctx, rec, stats)
	return dest, err
}

// PutRecordWithStats inserts a record and returns detailed statistics
func (mt *Memtable) PutRecordWithStats(ctx context.Context, rec *types.Record, stats *PutRecordStats) (string, error) {
	if !rec.Timestamp.IsZero() {
		return "", fmt.Errorf("record timestamp must be unset, will be assigned automatically")
	}

	// Estimate the size this record will consume
	estimatedSize := mt.estimateRecordSize(rec)
	
	// Check if adding this record would exceed the size limit
	currentSize := atomic.LoadInt64(&mt.activeSizeEstimate)
	if currentSize+estimatedSize > mt.maxSize {
		return "", &ErrMemtableFull{
			CurrentSize: currentSize,
			MaxSize:     mt.maxSize,
		}
	}

	c, err := mt.activeCollection(ctx)
	if err != nil {
		return "", err
	}

	for {
		rec.Timestamp = mt.clock.Now()

		_, err = c.InsertOne(ctx, rec)
		if err == nil {
			// Successfully inserted, update size estimate
			atomic.AddInt64(&mt.activeSizeEstimate, estimatedSize)
			break
		}

		// sleep and retry to get a new timestamp. it's almost certainly been
		// long enough already, but this makes testing easier.
		if mongo.IsDuplicateKeyError(err) {
			stats.RetryCount++
			jitter := time.Duration(rand.Int63n(retryJitter.Nanoseconds()))
			mt.clock.Sleep(retrySleep + jitter)
			continue
		}

		return "", err
	}

	return c.Name(), nil
}

// estimateRecordSize returns an approximate size in bytes for a record
func (mt *Memtable) estimateRecordSize(rec *types.Record) int64 {
	size := int64(len(rec.Key))
	if rec.Document != nil {
		size += int64(len(rec.Document))
	}
	size += recordOverheadBytes // BSON overhead, timestamp, etc.
	return size
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
	return mt.mongoClient.GetDB(ctx)
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
	
	// Reset size estimate for the new active memtable
	atomic.StoreInt64(&mt.activeSizeEstimate, 0)
	
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

// ErrStillReferenced is returned when attempting to drop a memtable that still has active references
var ErrStillReferenced = fmt.Errorf("memtable still has active references")

// TryDrop atomically checks if a memtable can be dropped and drops it if safe.
// This prevents the race condition between CanDropMemtable and Drop.
func (mt *Memtable) TryDrop(ctx context.Context, name string) error {
	mt.handlesMu.Lock()
	defer mt.handlesMu.Unlock()

	handle, exists := mt.handles[name]
	if !exists {
		// No handle exists, which means no active references.
		// Safe to drop directly.
		return mt.dropLocked(ctx, name)
	}

	// Atomic check-and-drop under lock
	if !handle.CanDrop() {
		return ErrStillReferenced
	}

	// Drop while holding lock to prevent new references
	err := mt.dropLocked(ctx, name)
	if err != nil {
		return err
	}

	// Remove from handle registry
	delete(mt.handles, name)
	return nil
}

// dropLocked performs the actual drop operation. Must be called with handlesMu held.
func (mt *Memtable) dropLocked(ctx context.Context, name string) error {
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

// Scan returns an iterator for all records in the memtable within the key range
func (mt *Memtable) Scan(ctx context.Context, collName, start, end string) (api.Iterator, error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetMongo: %w", err)
	}

	// build range query
	filter := bson.M{}
	if start != "" || end != "" {
		keyFilter := bson.M{}
		if start != "" {
			keyFilter["$gte"] = start
		}
		if end != "" {
			keyFilter["$lt"] = end
		}
		filter["key"] = keyFilter
	}

	// query sorted by key then by timestamp (newest first within each key)
	cursor, err := db.Collection(collName).Find(
		ctx,
		filter,
		options.Find().SetSort(bson.D{
			{Key: "key", Value: 1},
			{Key: "ts", Value: -1},
		}))
	if err != nil {
		return nil, fmt.Errorf("Find: %w", err)
	}

	return &memtableIterator{
		cursor: cursor,
		ctx:    ctx,
	}, nil
}

// ListMemtables returns all memtable names in creation order (newest first)
func (mt *Memtable) ListMemtables(ctx context.Context) ([]string, error) {
	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetMongo: %w", err)
	}

	cur, err := db.Collection(memtablesCollection).Find(
		ctx,
		bson.M{},
		options.Find().SetSort(bson.D{{Key: "created", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("db.Collection: %w", err)
	}
	defer cur.Close(ctx)

	var memtables []memtableInfo
	if err := cur.All(ctx, &memtables); err != nil {
		return nil, fmt.Errorf("cur.All: %w", err)
	}

	names := make([]string, len(memtables))
	for i, mt := range memtables {
		names[i] = mt.ID
	}
	return names, nil
}

// GetHandle returns a singleton handle for the specified memtable.
// The same handle is returned for multiple calls with the same name.
// This ensures reference counting works correctly across concurrent operations.
func (mt *Memtable) GetHandle(ctx context.Context, name string) (*Handle, error) {
	// Fast path: check if handle exists with read lock
	mt.handlesMu.RLock()
	if handle, exists := mt.handles[name]; exists {
		mt.handlesMu.RUnlock()
		return handle, nil
	}
	mt.handlesMu.RUnlock()

	// Slow path: create handle with write lock and double-check
	mt.handlesMu.Lock()
	defer mt.handlesMu.Unlock()
	
	// Double-check after acquiring write lock to handle race conditions:
	// Two goroutines could pass the read lock check above, then compete
	// for the write lock. The second one should use the handle created
	// by the first one rather than creating a duplicate.
	if handle, exists := mt.handles[name]; exists {
		return handle, nil
	}

	db, err := mt.GetMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetMongo: %w", err)
	}

	handle := NewHandle(db, name)
	mt.handles[name] = handle
	return handle, nil
}

// CanDropMemtable checks if a memtable can be safely dropped
func (mt *Memtable) CanDropMemtable(ctx context.Context, name string) (bool, error) {
	handle, err := mt.GetHandle(ctx, name)
	if err != nil {
		return false, err
	}
	return handle.CanDrop(), nil
}
