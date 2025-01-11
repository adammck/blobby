package blobstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/types"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Blobstore struct {
	bucket string
	s3     *s3.Client
}

func New(bucket string) *Blobstore {
	return &Blobstore{
		bucket: bucket,
	}
}

func (bs *Blobstore) Get(ctx context.Context, key string) (*types.Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (bs *Blobstore) Put(ctx context.Context, key string, value []byte) error {
	return fmt.Errorf("not implemented")
}

func (bs *Blobstore) Ping(ctx context.Context) error {
	_, err := bs.getS3(ctx)
	return err
}

func (bs *Blobstore) Init(ctx context.Context) error {
	return nil
}

var NoRecords = errors.New("NoRecords")

func (bs *Blobstore) Flush(ctx context.Context, ch chan *types.Record) (string, int, *sstable.Meta, error) {
	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", 0, nil, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w, err := sstable.NewWriter(f)
	if err != nil {
		return "", 0, nil, fmt.Errorf("sstable.NewWriter: %w", err)
	}

	n := 0
	for rec := range ch {
		err = w.Write(rec)
		if err != nil {
			return "", 0, nil, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	// nothing to write
	if n == 0 {
		return "", 0, nil, NoRecords
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, nil, fmt.Errorf("Seek: %w", err)
	}

	key := fmt.Sprintf("L1/%d.sstable", time.Now().Unix())
	_, err = bs.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		return "", 0, nil, fmt.Errorf("PutObject: %w", err)
	}

	fn := fmt.Sprintf("s3://%s/%s", bs.bucket, key)
	return fn, n, w.Meta(), nil
}

func (bs *Blobstore) getS3(ctx context.Context) (*s3.Client, error) {
	if bs.s3 != nil {
		return bs.s3, nil
	}

	s, err := connectToS3(ctx)
	if err != nil {
		return nil, err
	}

	bs.s3 = s
	return s, nil
}

func connectToS3(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return s3.NewFromConfig(cfg), nil
}
