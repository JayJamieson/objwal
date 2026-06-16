package objectstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Derived/mostly 1:1 copy from https://github.com/opendata-oss/opendata-go/blob/aa37f43069c2e512981fa63b2ebcbe2f657f82eb/objstore/memory.go
//
// InMemory is a thread-safe, in-process ObjectStore. It implements the same
// conditional-put semantics as a real store (monotonic per-object ETags), so
// the optimistic-concurrency paths in the buffer are exercised faithfully in
// tests.
type InMemory struct {
	mu      sync.Mutex
	objects map[string]memObject
	etagSeq uint64
}

type memObject struct {
	data         []byte
	etag         string
	lastModified time.Time
}

// NewInMemory returns an empty in-memory store.
func NewInMemory() *InMemory {
	return &InMemory{objects: make(map[string]memObject)}
}

func (s *InMemory) nextETagLocked() string {
	s.etagSeq++
	return fmt.Sprintf("etag-%d", s.etagSeq)
}

func (s *InMemory) Get(_ context.Context, path string) (GetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[path]
	if !ok {
		return GetResult{}, fmt.Errorf("%w: %s", ErrNotFound, path)
	}
	data := make([]byte, len(obj.data))
	copy(data, obj.data)
	return GetResult{
		Data: data,
		Meta: ObjectMeta{
			Location:     path,
			LastModified: obj.lastModified,
			Size:         int64(len(obj.data)),
			ETag:         obj.etag,
		},
	}, nil
}

func (s *InMemory) Put(ctx context.Context, path string, data []byte) error {
	return s.PutOpts(ctx, path, data, PutOptions{Mode: PutOverwrite})
}

func (s *InMemory) PutOpts(_ context.Context, path string, data []byte, opts PutOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.objects[path]
	switch opts.Mode {
	case PutCreate:
		if exists {
			return fmt.Errorf("%w: %s", ErrAlreadyExists, path)
		}
	case PutUpdate:
		if !exists || existing.etag != opts.Version.ETag {
			return fmt.Errorf("%w: %s", ErrPreconditionFailed, path)
		}
	case PutOverwrite:
		// unconditional
	}

	stored := make([]byte, len(data))
	copy(stored, data)
	s.objects[path] = memObject{
		data:         stored,
		etag:         s.nextETagLocked(),
		lastModified: time.Now(),
	}
	return nil
}

func (s *InMemory) List(_ context.Context, prefix string) ([]ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ObjectMeta
	for path, obj := range s.objects {
		if strings.HasPrefix(path, prefix) {
			out = append(out, ObjectMeta{
				Location:     path,
				LastModified: obj.lastModified,
				Size:         int64(len(obj.data)),
				ETag:         obj.etag,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Location < out[j].Location })
	return out, nil
}

func (s *InMemory) Delete(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[path]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, path)
	}
	delete(s.objects, path)
	return nil
}
