package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoAddr = flag.String("mongo", "mongodb://localhost:27017", "MongoDB connection string")
)

const (
	defaultDB = "archive"
)

type Record struct {
	Key       string    `bson:"_id"`
	Value     []byte    `bson:"value"`
	Timestamp time.Time `bson:"ts"`
}

type Archive struct {
	mongo *mongo.Database
	s3    *s3.Client
}

func (a *Archive) getMongo(ctx context.Context) (*mongo.Database, error) {
	if a.mongo != nil {
		return a.mongo, nil
	}

	m, err := connectToMongo(ctx)
	if err != nil {
		return nil, err
	}

	a.mongo = m
	return m, nil
}

func connectToMongo(ctx context.Context) (*mongo.Database, error) {
	opt := options.Client().ApplyURI(*mongoAddr).SetTimeout(1 * time.Second)

	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client.Database(defaultDB), nil
}

func (a *Archive) getS3(ctx context.Context) (*s3.Client, error) {
	if a.s3 != nil {
		return a.s3, nil
	}

	s, err := connectToS3(ctx)
	if err != nil {
		return nil, err
	}

	a.s3 = s
	return s, nil
}

func connectToS3(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return s3.NewFromConfig(cfg), nil
}

func NewArchive() *Archive {
	return &Archive{}
}

func main() {
	flag.Parse()

	ctx := context.Background()
	arc := NewArchive()

	_, err := arc.getMongo(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	_, err = arc.getS3(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to S3: %v", err)
	}
}
