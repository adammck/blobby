package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"

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

func (bs *Blobstore) Get(ctx context.Context, fn string, key string) (*types.Record, string, error) {
	reader, err := bs.getSST(ctx, fn)
	if err != nil {
		return nil, "", fmt.Errorf("getSST: %w", err)
	}

	for {
		rec, err := reader.Next()
		if err != nil {
			return nil, "", fmt.Errorf("Next: %w", err)
		}
		if rec == nil {
			// end of file
			return nil, "", nil
		}
		// TODO: index the file so we can grab a range
		if rec.Key == key {
			return rec, bs.url(fn), nil
		}
	}
}

func (bs *Blobstore) getSST(ctx context.Context, key string) (*sstable.Reader, error) {
	s3client, err := bs.getS3(ctx)
	if err != nil {
		return nil, err
	}

	output, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}

	reader, err := sstable.NewReader(output.Body)
	if err != nil {
		output.Body.Close()
		return nil, fmt.Errorf("NewReader: %w", err)
	}

	return reader, nil
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

	w := sstable.NewWriter()

	n := 0
	for rec := range ch {
		err = w.Add(rec)
		if err != nil {
			return "", 0, nil, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	// nothing to write
	if n == 0 {
		return "", 0, nil, NoRecords
	}

	meta, err := w.Write(f)
	if err != nil {
		return "", 0, nil, fmt.Errorf("sstable.Write: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, nil, fmt.Errorf("Seek: %w", err)
	}

	s3c, err := bs.getS3(ctx)
	if err != nil {
		return "", 0, nil, fmt.Errorf("getS3: %w", err)
	}

	key := meta.Filename()
	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		return "", 0, nil, fmt.Errorf("PutObject: %w", err)
	}

	return bs.url(key), n, meta, nil
}

func (bs *Blobstore) url(key string) string {
	return fmt.Sprintf("s3://%s/%s", bs.bucket, key)
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
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		// integration test needs this so we can just hit localhost rather than
		// the default of bucket-name.localhost, which doesn't work. seems fine
		// to just do this in production too.
		o.UsePathStyle = true
	}), nil
}
