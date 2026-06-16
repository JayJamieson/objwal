package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeS3 implements the narrow s3API with in-memory objects and faithful
// conditional-write semantics, so the adapter's header-setting and
// error-mapping are exercised without a live bucket. Errors are shaped so the
// adapter's isNotFound / isPreconditionFailed classifiers match (NoSuchKey via
// the typed error; precondition via the message), exactly as real S3 returns.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObj
	nextTag int
}

type fakeObj struct {
	data []byte
	etag string
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: map[string]fakeObj{}} }

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := *in.Key
	existing, exists := f.objects[key]

	if in.IfNoneMatch != nil { // create-only
		if exists {
			return nil, errors.New("api error PreconditionFailed: At least one of the pre-conditions you specified did not hold (status code: 412)")
		}
	}
	if in.IfMatch != nil { // update-if-version-matches
		want := strings.Trim(*in.IfMatch, `"`)
		if !exists || existing.etag != want {
			return nil, errors.New("api error PreconditionFailed: At least one of the pre-conditions you specified did not hold (status code: 412)")
		}
	}

	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.nextTag++
	etag := fmt.Sprintf("etag-%d", f.nextTag)
	f.objects[key] = fakeObj{data: data, etag: etag}
	quoted := `"` + etag + `"`
	return &s3.PutObjectOutput{ETag: &quoted}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[*in.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	quoted := `"` + o.etag + `"`
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(append([]byte(nil), o.data...))),
		ETag: &quoted,
	}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := ""
	if in.Prefix != nil {
		prefix = *in.Prefix
	}
	var contents []types.Object
	for k, o := range f.objects {
		if strings.HasPrefix(k, prefix) {
			key := k
			etag := `"` + o.etag + `"`
			size := int64(len(o.data))
			contents = append(contents, types.Object{Key: &key, ETag: &etag, Size: &size})
		}
	}
	truncated := false
	return &s3.ListObjectsV2Output{Contents: contents, IsTruncated: &truncated}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func newS3Adapter(f *fakeS3) *S3 { return &S3{client: f, bucket: "test-bucket"} }

func TestS3PutOptsCreateUpdatePreconditions(t *testing.T) {
	ctx := context.Background()
	s := newS3Adapter(newFakeS3())

	// Get on a missing key -> ErrNotFound.
	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}

	// PutCreate on a fresh key succeeds.
	if err := s.PutOpts(ctx, "k", []byte("v1"), PutOptions{Mode: PutCreate}); err != nil {
		t.Fatalf("PutCreate fresh: %v", err)
	}
	// PutCreate again -> ErrAlreadyExists.
	if err := s.PutOpts(ctx, "k", []byte("v2"), PutOptions{Mode: PutCreate}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("PutCreate existing: want ErrAlreadyExists, got %v", err)
	}

	// Get returns the value and a version.
	res, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Data) != "v1" {
		t.Fatalf("Get data = %q, want v1", res.Data)
	}
	if res.Meta.ETag == "" {
		t.Fatal("Get should return an ETag")
	}

	// PutUpdate with the right version succeeds.
	if err := s.PutOpts(ctx, "k", []byte("v3"), PutOptions{Mode: PutUpdate, Version: UpdateVersion{ETag: res.Meta.ETag}}); err != nil {
		t.Fatalf("PutUpdate matching: %v", err)
	}
	// PutUpdate with a stale version -> ErrPreconditionFailed.
	if err := s.PutOpts(ctx, "k", []byte("v4"), PutOptions{Mode: PutUpdate, Version: UpdateVersion{ETag: res.Meta.ETag}}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("PutUpdate stale: want ErrPreconditionFailed, got %v", err)
	}

	// Delete then Get -> ErrNotFound.
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: want ErrNotFound, got %v", err)
	}
}

func TestS3ListByPrefix(t *testing.T) {
	ctx := context.Background()
	s := newS3Adapter(newFakeS3())
	_ = s.Put(ctx, "wal/segments/a", []byte("1"))
	_ = s.Put(ctx, "wal/segments/b", []byte("2"))
	_ = s.Put(ctx, "wal/manifest", []byte("m"))

	segs, err := s.List(ctx, "wal/segments")
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 2 {
		t.Fatalf("List(wal/segments) = %d objects, want 2", len(segs))
	}
	man, err := s.List(ctx, "wal/manifest")
	if err != nil {
		t.Fatal(err)
	}
	if len(man) != 1 || man[0].Location != "wal/manifest" {
		t.Fatalf("List(wal/manifest) = %+v, want one wal/manifest", man)
	}
}
