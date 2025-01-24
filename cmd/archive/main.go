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

	"github.com/adammck/blobby/pkg/blobby"
	"github.com/adammck/blobby/pkg/compactor"
	"github.com/jonboulle/clockwork"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	ctx := context.Background()

	if len(os.Args) < 2 {
		fmt.Println("Usage: blobby <command> [arguments]")
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

	b := blobby.New(mongoURL, bucket, clockwork.NewRealClock())

	err := b.Ping(ctx)
	if err != nil {
		log.Fatalf("blobby.Ping: %v", err)
	}

	switch cmd {
	case "init":
		cmdInit(ctx, b)
	case "put":
		cmdPut(ctx, b, os.Stdin)
	case "get":
		cmdGet(ctx, b, os.Args[2])
	case "flush":
		cmdFlush(ctx, b)
	case "compact":
		cmdCompact(ctx, b, bucket)
	default:
		log.Fatalf("Unknown command: %s", cmd)
	}
}

type compactFlags struct {
	order    string
	minFiles int
	maxFiles int
	minSize  int64
	maxSize  int64
	minTime  string
	maxTime  string
}

func cmdCompact(ctx context.Context, b *blobby.Blobby, bucket string) {
	flags := flag.NewFlagSet("compact", flag.ExitOnError)
	cf := compactFlags{}

	flags.StringVar(&cf.order, "order", "oldest-first", "Order to compact files (oldest-first, newest-first, smallest-first, largest-first)")
	flags.IntVar(&cf.minFiles, "min-files", 2, "Minimum number of files to compact")
	flags.IntVar(&cf.maxFiles, "max-files", 0, "Maximum number of files to compact (0 for unlimited)")
	flags.Int64Var(&cf.minSize, "min-size", 0, "Minimum total input size in bytes")
	flags.Int64Var(&cf.maxSize, "max-size", 0, "Maximum total input size in bytes")
	flags.StringVar(&cf.minTime, "min-time", "", "Only include records newer than this (RFC3339)")
	flags.StringVar(&cf.maxTime, "max-time", "", "Only include records older than this (RFC3339)")

	flags.Parse(os.Args[2:])

	opts := compactor.CompactionOptions{
		MinFiles: cf.minFiles,
		MaxFiles: cf.maxFiles,
	}

	switch cf.order {
	case "oldest-first":
		opts.Order = compactor.OldestFirst
	case "newest-first":
		opts.Order = compactor.NewestFirst
	case "smallest-first":
		opts.Order = compactor.SmallestFirst
	case "largest-first":
		opts.Order = compactor.LargestFirst
	default:
		log.Fatalf("Invalid order: %s", cf.order)
	}

	if cf.minSize > 0 {
		opts.MinInputSize = int(cf.minSize)
	}
	if cf.maxSize > 0 {
		opts.MaxInputSize = int(cf.maxSize)
	}

	if cf.minTime != "" {
		t, err := time.Parse(time.RFC3339, cf.minTime)
		if err != nil {
			log.Fatalf("Invalid min-time: %v", err)
		}
		opts.MinTime = t
	}

	if cf.maxTime != "" {
		t, err := time.Parse(time.RFC3339, cf.maxTime)
		if err != nil {
			log.Fatalf("Invalid max-time: %v", err)
		}
		opts.MaxTime = t
	}

	stats, err := b.Compact(ctx, opts)
	if err != nil {
		log.Fatalf("Compact: %v", err)
	}

	for i, s := range stats {
		if s.Error != nil {
			fmt.Printf("Compaction %d failed: %v\n", i+1, s.Error)
			continue
		}

		fmt.Printf("Compaction %d:\n", i+1)
		fmt.Printf("  Input files: %d\n", len(s.Inputs))
		fmt.Printf("  Output files: %d\n", len(s.Outputs))
		for j, m := range s.Outputs {
			fmt.Printf("  Output %d: s3://%s/%s (%d records, %d bytes)\n", j+1, bucket, m.Filename(), m.Count, m.Size)
		}
	}
}

func cmdInit(ctx context.Context, b *blobby.Blobby) {
	err := b.Init(ctx)
	if err != nil {
		log.Fatalf("blobby.Init: %s", err)
	}

	fmt.Println("OK")
}

func cmdPut(ctx context.Context, b *blobby.Blobby, r io.Reader) {
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

		bb, err := bson.Marshal(doc)
		if err != nil {
			log.Fatalf("bson.Marshal: %s", err)
		}

		dest, err = b.Put(ctx, k, bb)
		if err != nil {
			log.Fatalf("Put: %s", err)
		}

		n += 1
	}

	fmt.Printf("Wrote %d documents to: %s\n", n, dest)
}

func cmdGet(ctx context.Context, b *blobby.Blobby, key string) {
	bb, stats, err := b.Get(ctx, key)
	if err != nil {
		log.Fatalf("Get: %s", err)
	}

	o := map[string]interface{}{}
	err = bson.Unmarshal(bb, &o)
	if err != nil {
		log.Fatalf("bson.Unmarshal: %s", err)
	}

	out, err := json.Marshal(o)
	if err != nil {
		log.Fatalf("json.Marshal: %s", err)
	}

	fmt.Fprintf(os.Stderr, "Scanned %d records in %d blobs\n", stats.RecordsScanned, stats.BlobsFetched)
	fmt.Fprintf(os.Stderr, "Found 1 record in: %s\n", stats.Source)
	fmt.Printf("%s\n", out)
}

func cmdFlush(ctx context.Context, b *blobby.Blobby) {
	stats, err := b.Flush(ctx)
	if err != nil {
		log.Fatalf("Flush: %s", err)
	}

	fmt.Printf("Flushed %d documents to: %s\n", stats.Meta.Count, stats.BlobURL)
	fmt.Printf("Active memtable is now: %s\n", stats.ActiveMemtable)
}
