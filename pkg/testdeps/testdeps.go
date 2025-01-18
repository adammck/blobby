package testdeps

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
)

const (
	accessKey = "minioadmin"
	secretKey = "minioadmin"
	bucket    = "test-bucket"
	region    = "us-east-1"
)

type Env struct {
	t   *testing.T
	cfg *config

	mongoURL string
	S3URI    string
	S3Bucket string
	S3Key    string
	S3Secret string

	containers []testcontainers.Container
}

type Option func(*config)

type config struct {
	useMongo bool
	useMinio bool
}

func WithMongo() Option {
	return func(c *config) {
		c.useMongo = true
	}
}

func WithMinio() Option {
	return func(c *config) {
		c.useMinio = true
	}
}

func New(ctx context.Context, t *testing.T, opts ...Option) *Env {
	t.Helper()

	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	env := &Env{
		cfg:      cfg,
		S3Bucket: bucket,
		S3Key:    accessKey,
		S3Secret: secretKey,
		t:        t,
	}

	if cfg.useMongo {
		env.startMongo(ctx)
	}

	if cfg.useMinio {
		env.startMinio(ctx)
	}

	t.Cleanup(func() {
		for _, c := range env.containers {
			c.Terminate(ctx)
		}
	})

	return env
}

// MongoURL returns the URL to the Mongo server, or fails the test if Mongo is
// not enabled. Use WithMongo to enable it.
func (e *Env) MongoURL() string {
	e.t.Helper()

	if !e.cfg.useMongo {
		e.t.Fatalf("mongo is not enabled; use WithMongo to enable it")
	}

	return e.mongoURL
}

func (e *Env) startMongo(ctx context.Context) {
	mongoC, err := tcmongo.Run(ctx,
		"mongo:6",
		tcmongo.WithReplicaSet("rs"))
	if err != nil {
		e.t.Fatalf("tcmongo.Run: %v", err)
	}

	e.containers = append(e.containers, mongoC)

	cs, err := mongoC.ConnectionString(ctx)
	if err != nil {
		e.t.Fatalf("ConnectionString: %v", err)
	}

	// Use direct connection, since we are using a single-node replset here.
	// Weird that this isn't included in the URL returned by ConnectionString
	// when WithReplicaSet is used.
	e.mongoURL = fmt.Sprintf("%s/?connect=direct", cs)
}

func (e *Env) startMinio(ctx context.Context) {
	minioC, err := runMinio(ctx)
	if err != nil {
		e.t.Fatalf("runMinio: %v", err)
	}
	e.containers = append(e.containers, minioC)

	minioPort, err := minioC.MappedPort(ctx, "9000/tcp")
	if err != nil {
		e.t.Fatalf("get minio port: %v", err)
	}
	e.S3URI = fmt.Sprintf("http://localhost:%s", minioPort.Port())

	// TODO: inject these via testEnv
	os.Setenv("AWS_ACCESS_KEY_ID", e.S3Key)
	os.Setenv("AWS_SECRET_ACCESS_KEY", e.S3Secret)
	os.Setenv("AWS_ENDPOINT_URL_S3", e.S3URI)
	os.Setenv("AWS_REGION", "auto")
}

func runMinio(ctx context.Context) (testcontainers.Container, error) {
	c, err := tcminio.Run(ctx,
		"minio/minio:latest",
		tcminio.WithUsername(accessKey),
		tcminio.WithPassword(secretKey))
	if err != nil {
		return nil, err
	}

	url, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	minioC, err := minio.New(url, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	err = minioC.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
	if err != nil {
		return nil, err
	}

	return c, nil
}
