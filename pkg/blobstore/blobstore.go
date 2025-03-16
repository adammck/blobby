package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jonboulle/clockwork"
)

var ErrNoRecords = errors.New("NoRecords")

type Blobstore struct {
	bucket string
	s3     *s3.Client
	clock  clockwork.Clock
}

func New(bucket string, clock clockwork.Clock) *Blobstore {
	return &Blobstore{
		bucket: bucket,
		clock:  clock,
	}
}

// GetFull reads a single SSTable from the blobstore.
// The caller must call Close on the reader when finished.
func (bs *Blobstore) GetFull(ctx context.Context, key string) (*sstable.Reader, error) {
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
		return nil, fmt.Errorf("NewReader: %w", err)
	}

	return reader, nil
}

// GetPartial reads a byte range of an SSTable from the blob store. This is much
// faster than reading the entire blob. The first and last args are inclusive,
// per RFC 7233
func (bs *Blobstore) GetPartial(ctx context.Context, key string, first, last int64) (*sstable.Reader, error) {
	s3c, err := bs.getS3(ctx)
	if err != nil {
		return nil, err
	}

	if first == 0 {
		return nil, fmt.Errorf("first must be greater than zero; use GetFull to read the entire blob")
	}

	rang := fmt.Sprintf("bytes=%d-", first)
	if last > 0 {
		rang += fmt.Sprintf("%d", last)
	}

	output, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
		Range:  aws.String(rang),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}

	reader := sstable.NewPartialReader(output.Body)
	return reader, nil
}

func (bs *Blobstore) Delete(ctx context.Context, key string) error {
	s3c, err := bs.getS3(ctx)
	if err != nil {
		return err
	}

	_, err = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("DeleteObject: %w", err)
	}

	return nil
}

func (bs *Blobstore) Ping(ctx context.Context) error {
	_, err := bs.getS3(ctx)
	return err
}

func (bs *Blobstore) Init(ctx context.Context) error {
	return nil
}

// TODO: remove most of the return values; meta contains everything.
func (bs *Blobstore) Flush(ctx context.Context, ch chan *types.Record, opts ...sstable.WriterOption) (dest string, count int, meta *sstable.Meta, index []api.IndexEntry, err error) {
	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w := sstable.NewWriter(bs.clock, opts...)

	n := 0
	for rec := range ch {
		err = w.Add(rec)
		if err != nil {
			return "", 0, nil, nil, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	// nothing to write
	if n == 0 {
		return "", 0, nil, nil, ErrNoRecords
	}

	meta, index, err = w.Write(f)
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("sstable.Write: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("Seek: %w", err)
	}

	s3c, err := bs.getS3(ctx)
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("getS3: %w", err)
	}

	key := meta.Filename()
	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bs.bucket,
		Key:    &key,
		Body:   f,
		// never overwrite sstables. they're immutable. this is only a problem
		// if we try to put two at the same time, since they're timestamped.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return "", 0, nil, nil, fmt.Errorf("PutObject: %w", err)
	}

	return key, n, meta, index, nil
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
