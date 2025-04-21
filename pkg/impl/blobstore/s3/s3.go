package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/adammck/blobby/pkg/api"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	bucket string
	s3     *s3.Client
}

var _ api.BlobStore = (*Store)(nil)

func New(bucket string) *Store {
	return &Store{bucket: bucket}
}

func (s *Store) Put(ctx context.Context, key string, data io.Reader) error {
	s3c, err := s.getS3(ctx)
	if err != nil {
		return err
	}

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        data,
		IfNoneMatch: aws.String("*"), // never overwrite
	})
	if err != nil {
		return fmt.Errorf("PutObject: %w", err)
	}

	return nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s3c, err := s.getS3(ctx)
	if err != nil {
		return nil, err
	}

	output, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}

	return output.Body, nil
}

func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s3c, err := s.getS3(ctx)
	if err != nil {
		return nil, err
	}

	rang := fmt.Sprintf("bytes=%d-", start)
	if end > 0 {
		rang += fmt.Sprintf("%d", end)
	}

	output, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Range:  aws.String(rang),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}

	return output.Body, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	s3c, err := s.getS3(ctx)
	if err != nil {
		return err
	}

	_, err = s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("DeleteObject: %w", err)
	}

	return nil
}

func (s *Store) getS3(ctx context.Context) (*s3.Client, error) {
	if s.s3 != nil {
		return s.s3, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	s.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return s.s3, nil
}
