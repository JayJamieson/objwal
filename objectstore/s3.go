package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Derived/mostly 1:1 copy from https://github.com/opendata-oss/opendata-go/blob/aa37f43069c2e512981fa63b2ebcbe2f657f82eb/objstore/s3.go
//
// S3 is an ObjectStore backed by AWS S3 or an S3-compatible store (MinIO, etc.).
//
// Conditional writes rely on S3's If-None-Match / If-Match preconditions:
// PutCreate sends If-None-Match: "*", PutUpdate sends If-Match: <etag>. These
// are supported by AWS S3 (since Nov 2024) and by recent MinIO releases; an
// older S3-compatible store that ignores the preconditions will silently break
// the CAS protocol, so verify support before relying on it.
//
// Adapted from the opendata-go objstore bindings, mapped onto this package's
// PutOpts / List / ObjectMeta surface.
// s3API is the narrow subset of the S3 client the adapter uses. *s3.Client
// satisfies it; tests inject a fake with in-memory conditional-write semantics
// so the adapter's header-setting and error-mapping are covered without a live
// bucket.
type s3API interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type S3 struct {
	client s3API
	bucket string
}

// NewS3 binds an S3 ObjectStore to a bucket using an existing S3 client. The
// caller configures the client (region, credentials, and for MinIO the
// BaseEndpoint + UsePathStyle).
func NewS3(client *s3.Client, bucket string) *S3 {
	return &S3{client: client, bucket: bucket}
}

// Get implements ObjectStore.
func (s *S3) Get(ctx context.Context, path string) (GetResult, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	if err != nil {
		if isNotFound(err) {
			return GetResult{}, ErrNotFound
		}
		return GetResult{}, err
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return GetResult{}, err
	}
	meta := ObjectMeta{Location: path, Size: int64(len(data))}
	if out.ETag != nil {
		meta.ETag = strings.Trim(*out.ETag, `"`)
	}
	if out.LastModified != nil {
		meta.LastModified = *out.LastModified
	}
	if out.VersionId != nil {
		meta.Version = *out.VersionId
	}
	return GetResult{Data: data, Meta: meta}, nil
}

// Put implements ObjectStore (unconditional).
//
// The body is wrapped in a bytes.Reader rather than copied through a string;
// for multi-megabyte segments that copy would dominate the PUT's memory cost.
func (s *S3) Put(ctx context.Context, path string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   bytes.NewReader(data),
	})
	return err
}

// PutOpts implements ObjectStore. PutCreate maps to If-None-Match: "*" and a
// create conflict to ErrAlreadyExists; PutUpdate maps to If-Match and a version
// mismatch to ErrPreconditionFailed.
func (s *S3) PutOpts(ctx context.Context, path string, data []byte, opts PutOptions) error {
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   bytes.NewReader(data),
	}
	switch opts.Mode {
	case PutCreate:
		input.IfNoneMatch = aws.String("*")
	case PutUpdate:
		etag := opts.Version.ETag
		if etag != "" && !strings.HasPrefix(etag, `"`) {
			etag = `"` + etag + `"`
		}
		input.IfMatch = aws.String(etag)
	case PutOverwrite:
		// no precondition
	}
	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			if opts.Mode == PutCreate {
				return ErrAlreadyExists
			}
			return ErrPreconditionFailed
		}
		return err
	}
	return nil
}

// List implements ObjectStore, paginating ListObjectsV2.
func (s *S3) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	var out []ObjectMeta
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &s.bucket,
			Prefix:            &prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			m := ObjectMeta{}
			if o.Key != nil {
				m.Location = *o.Key
			}
			if o.Size != nil {
				m.Size = *o.Size
			}
			if o.ETag != nil {
				m.ETag = strings.Trim(*o.ETag, `"`)
			}
			if o.LastModified != nil {
				m.LastModified = *o.LastModified
			}
			out = append(out, m)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	return out, nil
}

// Delete implements ObjectStore. Deleting a missing object is not an error on
// S3, which matches this package's contract for callers that ignore ErrNotFound.
func (s *S3) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	return err
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "StatusCode: 404")
}

func isPreconditionFailed(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "PreconditionFailed") ||
		strings.Contains(msg, "StatusCode: 412") ||
		strings.Contains(msg, "StatusCode: 409") ||
		strings.Contains(msg, "ConditionalRequestConflict") ||
		strings.Contains(msg, "At least one of the pre-conditions")
}

// compile-time assertions.
var (
	_ ObjectStore = (*S3)(nil)
	_             = time.Time{}
)
