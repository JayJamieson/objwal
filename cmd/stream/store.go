package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/JayJamieson/objwal/objectstore"
)

// newStore builds an objectstore.S3 over a MinIO/S3 endpoint, optionally
// ensuring the bucket exists. Path-style addressing and an explicit endpoint are
// mandatory for MinIO.
func newStore(ctx context.Context, c *Config) (*objectstore.S3, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = c.PathStyle
	})
	if c.CreateBucket {
		if err := ensureBucket(ctx, client, c.Bucket); err != nil {
			return nil, err
		}
	}
	return objectstore.NewS3(client, c.Bucket), nil
}

// ensureBucket creates the bucket if HeadBucket reports it missing, treating
// "already owned" / "already exists" as success.
func ensureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return fmt.Errorf("ensure bucket %q: %w", bucket, err)
}
