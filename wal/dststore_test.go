package wal_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/JayJamieson/objwal/objectstore"
)

// backingStore returns the store the linearizability harness runs against.
//
// By default that is the in-memory store, which is linearizable by
// construction (a mutex around a map) - so the harness is checking objwal's
// protocol logic given a perfect CAS register, and nothing about whether a
// real store provides one.
//
// Set OBJWAL_TEST_ENDPOINT to point it at a live S3-compatible store instead
// (MinIO, R2, Tigris, real S3). Then the same model is checking the store's
// conditional-write conformance as well as objwal's use of it.
//
//	OBJWAL_TEST_ENDPOINT=http://127.0.0.1:9000 \
//	OBJWAL_TEST_BUCKET=objwal-dst \
//	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//	go test ./wal/ -run TestLinearizable
//
// Each run gets a fresh random key prefix so runs never interfere.
func backingStore(t *testing.T) (objectstore.ObjectStore, string) {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "dst-" + hex.EncodeToString(b[:]) + "/"

	endpoint := os.Getenv("OBJWAL_TEST_ENDPOINT")
	bucket := os.Getenv("OBJWAL_TEST_BUCKET")
	if endpoint == "" && bucket == "" {
		return objectstore.NewInMemory(), prefix
	}
	if bucket == "" {
		t.Fatal("OBJWAL_TEST_ENDPOINT set without OBJWAL_TEST_BUCKET")
	}
	ctx := context.Background()
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(envOr("AWS_REGION", "us-east-1"))}
	if k := os.Getenv("AWS_ACCESS_KEY_ID"); k != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(k, os.Getenv("AWS_SECRET_ACCESS_KEY"), "")))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return objectstore.NewS3(client, bucket), prefix
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
