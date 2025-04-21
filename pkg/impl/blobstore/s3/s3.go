// pkg/impl/blobstore/awss3/blobstore.go
package s3

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/sstable"
	"github.com/adammck/blobby/pkg/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jonboulle/clockwork"
)

type BlobStore struct {
	bucket  string
	s3      *s3.Client
	clock   clockwork.Clock
	factory sstable.Factory
}

var _ api.BlobStore = (*BlobStore)(nil) // Type check: implements interface

func New(bucket string, clock clockwork.Clock, factory sstable.Factory) *BlobStore {
	return &BlobStore{
		bucket:  bucket,
		clock:   clock,
		factory: factory,
	}
}

func (bs *BlobStore) GetFull(ctx context.Context, key string) (io.ReadCloser, error) {
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

	return output.Body, nil
}

func (bs *BlobStore) GetPartial(ctx context.Context, key string, first, last int64) (io.ReadCloser, error) {
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

	return output.Body, nil
}

func (bs *BlobStore) Delete(ctx context.Context, key string) error {
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

func (bs *BlobStore) Ping(ctx context.Context) error {
	_, err := bs.getS3(ctx)
	return err
}

func (bs *BlobStore) Flush(ctx context.Context, ch <-chan *types.Record) (dest string, count int, meta *api.BlobMeta, index []api.IndexEntry, filter api.Filter, err error) {
	f, err := os.CreateTemp("", "sstable-*")
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("CreateTemp: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	w := bs.factory.NewWriter()
	n := 0
	for rec := range ch {
		err = w.Add(rec)
		if err != nil {
			return "", 0, nil, nil, nil, fmt.Errorf("Write: %w", err)
		}
		n++
	}

	if n == 0 {
		return "", 0, nil, nil, nil, api.ErrNoRecords
	}

	blobMeta, blobIndex, blobFilter, err := w.Write(f)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("sstable.Write: %w", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("Seek: %w", err)
	}

	s3c, err := bs.getS3(ctx)
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("getS3: %w", err)
	}

	key := blobMeta.Filename()
	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bs.bucket,
		Key:         &key,
		Body:        f,
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return "", 0, nil, nil, nil, fmt.Errorf("PutObject: %w", err)
	}

	return key, n, blobMeta, blobIndex, blobFilter, nil
}

func (bs *BlobStore) getS3(ctx context.Context) (*s3.Client, error) {
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
		o.UsePathStyle = true
	}), nil
}
