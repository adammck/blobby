package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/adammck/archive/pkg/archive"
	"go.mongodb.org/mongo-driver/bson"
)

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

	arc := archive.New(mongoURL, bucket)

	err := arc.Ping(ctx)
	if err != nil {
		log.Fatalf("archive.Ping: %v", err)
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

func cmdInit(ctx context.Context, arc *archive.Archive) {
	err := arc.Init(ctx)
	if err != nil {
		log.Fatalf("archive.Init: %s", err)
	}

	fmt.Println("OK")
}

func cmdPut(ctx context.Context, arc *archive.Archive, r io.Reader) {
	n := 0
	var dest string
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

		dest, err = arc.Put(ctx, k, b)
		if err != nil {
			log.Fatalf("Put: %s", err)
		}

		n += 1
	}

	fmt.Printf("Wrote %d documents to: %s\n", n, dest)
}

func cmdGet(ctx context.Context, arc *archive.Archive, key string) {
	b, src, err := arc.Get(ctx, key)
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

	fmt.Fprintf(os.Stderr, "Got 1 record from: %s\n", src)
	fmt.Printf("%s\n", out)
}

func cmdFlush(ctx context.Context, arc *archive.Archive) {
	fn, n, mt, err := arc.Flush(ctx)
	if err != nil {
		log.Fatalf("Flush: %s", err)
	}

	fmt.Printf("Flushed %d documents to: %s\n", n, fn)
	fmt.Printf("Active memtable is now: %s\n", mt)
}
