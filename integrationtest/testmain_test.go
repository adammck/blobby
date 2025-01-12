package integrationtest

import (
	"context"
	"fmt"
	"log"
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
)

type testEnvT struct {
	mongoURI   string
	s3URI      string
	s3Bucket   string
	s3Key      string
	s3Secret   string
	containers []testcontainers.Container
}

var testEnv *testEnvT

func TestMain(m *testing.M) {
	ctx := context.Background()

	if os.Getenv("SKIP_INTEGRATION") == "1" {
		log.Printf("Skipping integration tests")
		os.Exit(0)
	}

	e, err := setupTestEnv(ctx)
	if err != nil {
		log.Printf("setupTestEnv: %v", err)
		os.Exit(1)
	}

	// TODO: inject these via testEnv
	os.Setenv("AWS_ACCESS_KEY_ID", e.s3Key)
	os.Setenv("AWS_SECRET_ACCESS_KEY", e.s3Secret)
	os.Setenv("AWS_ENDPOINT_URL_S3", e.s3URI)
	os.Setenv("AWS_REGION", "auto")

	testEnv = e

	code := m.Run()
	for _, c := range e.containers {
		testcontainers.TerminateContainer(c)
	}

	os.Exit(code)
}

func setupTestEnv(ctx context.Context) (*testEnvT, error) {
	mongoC, err := startMongo(ctx)
	if err != nil {
		return nil, err
	}

	mongoPort, err := mongoC.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return nil, err
	}

	minioC, err := startMinio(ctx)
	if err != nil {
		return nil, err
	}

	minioPort, err := minioC.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return nil, err
	}

	return &testEnvT{
		mongoURI: fmt.Sprintf("mongodb://localhost:%s", mongoPort.Port()),
		s3URI:    fmt.Sprintf("http://localhost:%s", minioPort.Port()),
		s3Bucket: bucket,
		s3Key:    accessKey,
		s3Secret: secretKey,
		containers: []testcontainers.Container{
			mongoC,
			minioC,
		},
	}, nil
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
		tcminio.WithUsername(accessKey), tcminio.WithPassword(secretKey))
	if err != nil {
		return nil, err
	}

	// c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
	// 	Started: true,
	// 	ContainerRequest: testcontainers.ContainerRequest{
	// 		Image:        "minio/minio:latest",
	// 		Cmd:          []string{"server", "/data"},
	// 		ExposedPorts: []string{"9000/tcp"},
	// 		Env: map[string]string{
	// 			"MINIO_ACCESS_KEY": accessKey,
	// 			"MINIO_SECRET_KEY": secretKey,
	// 		},
	// 		WaitingFor: wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp"),
	// 	},
	// })
	// if err != nil {
	// 	return nil, err
	// }

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

	err = minioC.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"})
	if err != nil {
		return nil, err
	}

	return c, nil
}
