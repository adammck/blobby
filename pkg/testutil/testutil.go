package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	accessKey = "minioadmin"
	secretKey = "minioadmin"
	bucket    = "test-bucket"
	region    = "us-east-1"
)

type Env struct {
	MongoURL   string
	S3URI      string
	S3Bucket   string
	S3Key      string
	S3Secret   string
	containers []testcontainers.Container
}

func SetupTest(ctx context.Context, t *testing.T) *Env {
	t.Helper()

	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	env, err := setupTestEnv(ctx)
	if err != nil {
		t.Fatalf("SetupTestEnv: %v", err)
	}

	t.Cleanup(func() {
		for _, c := range env.containers {
			c.Terminate(ctx)
		}
	})

	return env
}

func setupTestEnv(ctx context.Context) (*Env, error) {
	mongoC, err := startMongo(ctx)
	if err != nil {
		return nil, fmt.Errorf("startMongo: %w", err)
	}

	mongoPort, err := mongoC.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return nil, fmt.Errorf("get mongo port: %w", err)
	}

	minioC, err := startMinio(ctx)
	if err != nil {
		return nil, fmt.Errorf("startMinio: %w", err)
	}

	minioPort, err := minioC.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return nil, fmt.Errorf("get minio port: %w", err)
	}

	env := &Env{
		MongoURL: fmt.Sprintf("mongodb://localhost:%s", mongoPort.Port()),
		S3URI:    fmt.Sprintf("http://localhost:%s", minioPort.Port()),
		S3Bucket: bucket,
		S3Key:    accessKey,
		S3Secret: secretKey,
		containers: []testcontainers.Container{
			mongoC,
			minioC,
		},
	}

	// TODO: inject these via testEnv
	os.Setenv("AWS_ACCESS_KEY_ID", env.S3Key)
	os.Setenv("AWS_SECRET_ACCESS_KEY", env.S3Secret)
	os.Setenv("AWS_ENDPOINT_URL_S3", env.S3URI)
	os.Setenv("AWS_REGION", "auto")

	return env, nil
}

func startMongo(ctx context.Context) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mongo:6",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort("27017/tcp"),
		},
	})
}

func startMinio(ctx context.Context) (testcontainers.Container, error) {
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
