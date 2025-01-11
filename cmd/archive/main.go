package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/adammck/archive/pkg/blobstore"
	"github.com/adammck/archive/pkg/memtable"
	"github.com/adammck/archive/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
)

type Archive struct {
	mt *memtable.Memtable
	bs *blobstore.Blobstore
}

func NewArchive(mongoURL, bucket string) *Archive {
	return &Archive{
		mt: memtable.New(mongoURL),
		bs: blobstore.New(bucket),
	}
}

func (a *Archive) Put(ctx context.Context, key string, value []byte) error {
	return a.mt.Put(ctx, key, value)
}

func (a *Archive) Get(ctx context.Context, key string) ([]byte, error) {
	rec, err := a.mt.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return rec.Document, nil
	}

	panic("s3 not implemented")
}

func (a *Archive) Flush(ctx context.Context) (string, int, string, error) {

	// TODO: check whether old sstable is still flushing
	handle, mt, err := a.mt.Swap(ctx)
	if err != nil {
		return "", 0, "", fmt.Errorf("switchMemtable: %s", err)
	}

	ch := make(chan *types.Record)
	g, ctx2 := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		err = handle.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("memtable.Flush: %w", err)
		}
		return nil
	})

	var fn string
	var n int

	g.Go(func() error {
		var err error
		fn, n, err = a.bs.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		return "", 0, "", err
	}

	err = handle.Truncate(ctx)
	if err != nil {
		return "", 0, "", fmt.Errorf("handle.Truncate: %w", err)
	}

	return fn, n, mt, nil
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

	err := arc.mt.Ping(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	err = arc.bs.Ping(ctx)
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
	err := arc.mt.Init(ctx)
	if err != nil {
		log.Fatalf("getMongo: %s", err)
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
	fn, n, mt, err := arc.Flush(ctx)
	if err != nil {
		log.Fatalf("Flush: %s", err)
	}

	fmt.Printf("Flushed %d documents to: %s\n", n, fn)
	fmt.Printf("Active memtable is now: %s\n", mt)
}
