package objectstore

import (
	"context"
	"errors"
	"time"
)

// Derived/mostly 1:1 copy from https://github.com/opendata-oss/opendata-go/blob/aa37f43069c2e512981fa63b2ebcbe2f657f82eb/objstore/objstore.go
//
// Package objectstore defines the minimal object-storage abstraction the
// buffer needs: GET, unconditional PUT, conditional PUT (create-if-absent and
// compare-and-swap on version), LIST, and DELETE.
//
// The conditional PUT is the load-bearing primitive: the queue manifest is a
// single object mutated with optimistic concurrency, so the backing store
// must support either ETag-based If-Match/If-None-Match.

var (
	// ErrNotFound is returned by Get/Delete when the object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrPreconditionFailed is returned by a conditional Put whose version
	// check failed (the object changed since it was read).
	ErrPreconditionFailed = errors.New("precondition failed")
	// ErrAlreadyExists is returned by a create-mode Put when the object
	// already exists.
	ErrAlreadyExists = errors.New("object already exists")
	// ErrConflict is a rejected conditional write caused by another
	// conditional write in flight (S3 409 ConditionalRequestConflict). The
	// write did not land, and unlike ErrPreconditionFailed it says nothing
	// about the object's current version: retry with backoff.
	ErrConflict = errors.New("conditional request conflict")
)

// UpdateVersion identifies the version of an object as observed by a Get,
// for use as a precondition in a later conditional Put. Either field may be
// empty depending on what the backing store provides.
type UpdateVersion struct {
	ETag    string
	Version string
}

type ObjectMeta struct {
	Location     string
	LastModified time.Time
	Size         int64
	ETag         string
	Version      string
}

type GetResult struct {
	Data []byte
	Meta ObjectMeta
}

type PutMode int

const (
	// PutOverwrite writes unconditionally.
	PutOverwrite PutMode = iota
	// PutCreate writes only if the object does not already exist
	// (If-None-Match: *). Fails with ErrAlreadyExists.
	PutCreate
	// PutUpdate writes only if the object's current version matches the
	// supplied UpdateVersion (If-Match). Fails with ErrPreconditionFailed.
	PutUpdate
)

// PutOptions carries the mode and, for PutUpdate, the expected version.
type PutOptions struct {
	Mode    PutMode
	Version UpdateVersion
}

type ObjectStore interface {
	// Get returns the full object at path, or ErrNotFound.
	Get(ctx context.Context, path string) (GetResult, error)

	// Put writes data to path unconditionally.
	Put(ctx context.Context, path string, data []byte) error

	// PutOpts writes data to path subject to opts. PutCreate fails with
	// ErrAlreadyExists; PutUpdate fails with ErrPreconditionFailed when the
	// stored version no longer matches.
	PutOpts(ctx context.Context, path string, data []byte, opts PutOptions) error

	// List returns metadata for all objects whose path begins with prefix.
	List(ctx context.Context, prefix string) ([]ObjectMeta, error)

	// Delete removes the object at path. Deleting a missing object returns
	// ErrNotFound (callers that want idempotent deletes can ignore it).
	Delete(ctx context.Context, path string) error
}
