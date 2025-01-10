package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultDB               = "archive"
	metaCollectionName      = "meta"
	metaActiveMemtableDocID = "active_memtable"
	blueMemtableName        = "blue"
	greenMemtableName       = "green"
)

type Record struct {
	Key       string    `bson:"key"`
	Timestamp time.Time `bson:"ts"`
	Document  []byte    `bson:"doc"`
}

type Archive struct {
	mongoURL string
	bucket   string

	mongo *mongo.Database
	s3    *s3.Client
}

func (a *Archive) getMongo(ctx context.Context) (*mongo.Database, error) {
	if a.mongo != nil {
		return a.mongo, nil
	}

	m, err := connectToMongo(ctx, a.mongoURL)
	if err != nil {
		return nil, err
	}

	a.mongo = m
	return m, nil
}

func connectToMongo(ctx context.Context, url string) (*mongo.Database, error) {
	opt := options.Client().ApplyURI(url).SetTimeout(1 * time.Second)

	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client.Database(defaultDB), nil
}

func (a *Archive) getS3(ctx context.Context) (*s3.Client, error) {
	if a.s3 != nil {
		return a.s3, nil
	}

	s, err := connectToS3(ctx)
	if err != nil {
		return nil, err
	}

	a.s3 = s
	return s, nil
}

func connectToS3(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return s3.NewFromConfig(cfg), nil
}

func NewArchive(mongoURL, bucket string) *Archive {
	return &Archive{
		mongoURL: mongoURL,
		bucket:   bucket,
	}
}

func (a *Archive) activeMemtableName(ctx context.Context, m *mongo.Database) (string, error) {
	res := m.Collection(metaCollectionName).FindOne(ctx, bson.M{"_id": metaActiveMemtableDocID})

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

func (a *Archive) activeMemtable(ctx context.Context) (*mongo.Collection, error) {
	m, err := a.getMongo(ctx)
	if err != nil {
		return nil, err
	}

	cn, err := a.activeMemtableName(ctx, m)
	if err != nil {
		return nil, err
	}

	return m.Collection(cn), nil
}

func (a *Archive) Put(ctx context.Context, key string, value []byte) error {
	c, err := a.activeMemtable(ctx)
	if err != nil {
		return err
	}

	_, err = c.InsertOne(ctx, &Record{
		Key:       key,
		Timestamp: time.Now(), // TODO: inject a clock
		Document:  value,
	})
	return err
}

func (a *Archive) Get(ctx context.Context, key string) ([]byte, error) {
	rec, err := a.getFromMongo(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return rec.Document, nil
	}

	panic("s3 not implemented")
}

func (a *Archive) getFromMongo(ctx context.Context, key string) (*Record, error) {
	c, err := a.activeMemtable(ctx)
	if err != nil {
		return nil, err
	}

	res := c.FindOne(ctx, bson.M{"key": key}, options.FindOne().SetSort(bson.M{"ts": -1}))

	b, err := res.Raw()
	if err != nil {

		// this is actually fine
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}

		return nil, fmt.Errorf("FindOne: %w", err)
	}

	var rec Record
	err = bson.Unmarshal(b, &rec)
	if err != nil {
		return nil, fmt.Errorf("error decoding record: %w", err)
	}

	return &rec, nil
}

func (a *Archive) Flush(ctx context.Context) (string, int, error) {
	db, err := a.getMongo(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("getMongo: %s", err)
	}

	// TODO: check whether old sstable is still flushing
	prev, err := a.swapMemtable(ctx, db)
	if err != nil {
		return "", 0, fmt.Errorf("switchMemtable: %s", err)
	}

	return a.flush(ctx, db, prev)
}

func (a *Archive) swapMemtable(ctx context.Context, m *mongo.Database) (string, error) {
	curr, err := a.activeMemtableName(ctx, m)
	if err != nil {
		return "", err
	}

	next := blueMemtableName
	if curr == blueMemtableName {
		next = greenMemtableName
	}

	_, err = m.Collection(metaCollectionName).UpdateOne(
		ctx,
		bson.M{"_id": metaActiveMemtableDocID},
		bson.M{"$set": bson.M{"value": next}},
	)
	if err != nil {
		return "", fmt.Errorf("error updating active memtable: %w", err)
	}

	return curr, nil
}

func (a *Archive) flush(ctx context.Context, db *mongo.Database, memtable string) (string, int, error) {
	coll := db.Collection(memtable)

	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return "", 0, fmt.Errorf("Find: %w", err)
	}
	defer cur.Close(ctx)

	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", 0, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w, err := NewSSTableWriter(f)
	if err != nil {
		return "", 0, fmt.Errorf("NewSSTableWriter: %w", err)
	}

	n := 0
	for cur.Next(ctx) {
		var rec Record
		err = cur.Decode(&rec)
		if err != nil {
			return "", 0, fmt.Errorf("Decode: %w", err)
		}

		err = w.Write(&rec)
		if err != nil {
			return "", 0, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	err = cur.Err()
	if err != nil {
		return "", 0, fmt.Errorf("cursor error: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, fmt.Errorf("Seek: %w", err)
	}

	k := fmt.Sprintf("L1/%d.sstable", time.Now().Unix())
	_, err = a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &a.bucket,
		Key:    &k,
		Body:   f,
	})
	if err != nil {
		return "", 0, fmt.Errorf("PutObject: %w", err)
	}

	// TODO: recreate collection and indices here.
	err = coll.Drop(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("Drop: %w", err)
	}

	fn := fmt.Sprintf("s3://%s/%s", a.bucket, k)
	return fn, n, nil
}

func main() {
	ctx := context.Background()

	if len(os.Args) < 2 {
		fmt.Println("Usage: archive <command> [arguments]")
		os.Exit(1)
	}

	cmd := os.Args[1]
	flag.Parse()

	mongoURL := os.Getenv("MONGO_URL")
	if mongoURL == "" {
		log.Fatalf("Required: MONGO_URL")
	}

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		log.Fatalf("Required: S3_BUCKET")
	}

	arc := NewArchive(mongoURL, bucket)

	_, err := arc.getMongo(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	_, err = arc.getS3(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to S3: %v", err)
	}

	switch cmd {
	case "init":
		cmdInit(ctx, arc)
	case "put":
		cmdPut(ctx, arc, os.Stdin)
	case "get":
		cmdGet(ctx, arc, os.Args[2])
	case "flush":
		cmdFlush(ctx, arc)
	default:
		log.Fatalf("Unknown command: %s", cmd)
	}
}

func cmdInit(ctx context.Context, arc *Archive) {
	db, err := arc.getMongo(ctx)
	if err != nil {
		log.Fatalf("getMongo: %s", err)
	}

	err = db.CreateCollection(ctx, metaCollectionName)
	if err != nil {
		log.Fatalf("CreateCollection: %s", err)
	}

	coll := db.Collection(metaCollectionName)
	_, err = coll.InsertOne(ctx, bson.M{"_id": metaActiveMemtableDocID, "value": blueMemtableName})
	if err != nil {
		log.Fatalf("InsertOne: %s", err)
	}

	err = db.CreateCollection(ctx, blueMemtableName)
	if err != nil {
		log.Fatalf("CreateCollection: %s", err)
	}
	cb := db.Collection(blueMemtableName)
	_, err = cb.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "key", Value: 1},
			{Key: "ts", Value: -1},
		},
	})
	if err != nil {
		log.Fatalf("CreateIndex (blue): %s", err)
	}

	err = db.CreateCollection(ctx, greenMemtableName)
	if err != nil {
		log.Fatalf("CreateCollection: %s", err)
	}
	cg := db.Collection(greenMemtableName)
	_, err = cg.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "key", Value: 1},
			{Key: "ts", Value: -1},
		},
	})
	if err != nil {
		log.Fatalf("CreateIndex (green): %s", err)
	}

	fmt.Println("OK")
}

func cmdPut(ctx context.Context, arc *Archive, r io.Reader) {
	n := 0
	dec := json.NewDecoder(r)
	for {
		var doc map[string]interface{}
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Decode: %s", err)
		}

		id, ok := doc["_id"]
		if !ok {
			log.Fatalf("Document missing _id field")
		}
		k := fmt.Sprintf("%v", id)

		b, err := bson.Marshal(doc)
		if err != nil {
			log.Fatalf("bson.Marshal: %s", err)
		}

		err = arc.Put(ctx, k, b)
		if err != nil {
			log.Fatalf("Put: %s", err)
		}

		n += 1
	}

	fmt.Printf("Wrote %d documents\n", n)
}

func cmdGet(ctx context.Context, arc *Archive, key string) {
	b, err := arc.Get(ctx, key)
	if err != nil {
		log.Fatalf("Get: %s", err)
	}

	o := map[string]interface{}{}
	err = bson.Unmarshal(b, &o)
	if err != nil {
		log.Fatalf("bson.Unmarshal: %s", err)
	}

	out, err := json.Marshal(o)
	if err != nil {
		log.Fatalf("json.Marshal: %s", err)
	}

	fmt.Printf("%s\n", out)
}

func cmdFlush(ctx context.Context, arc *Archive) {
	fn, n, err := arc.Flush(ctx)
	if err != nil {
		log.Fatalf("Flush: %s", err)
	}

	fmt.Printf("Flushed %d documents to: %s", n, fn)
}
