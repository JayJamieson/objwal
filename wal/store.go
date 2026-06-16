package wal

import (
	"context"
	"errors"

	"github.com/JayJamieson/objwal/objectstore"
)

// Store is the CAS read/commit seam for a manifest object. The writer uses
// Commit (PutUpdate against the version observed by Load); the reader uses
// Load to tail. It is deliberately thin: on a precondition failure the caller
// re-Loads to get the fresh version and replans, exactly as the buffer does.
type Store struct {
	os   objectstore.ObjectStore
	path string
}

// NewStore binds a Store to a manifest path in the given object store.
func NewStore(os objectstore.ObjectStore, path string) *Store {
	return &Store{os: os, path: path}
}

// Load fetches and parses the manifest, returning its current version for use
// as a CAS precondition. If the manifest does not exist yet it returns a fresh
// empty manifest, a nil version, and ok=false.
func (s *Store) Load(ctx context.Context) (m *Manifest, version *objectstore.UpdateVersion, ok bool, err error) {
	res, err := s.os.Get(ctx, s.path)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return NewManifest(), nil, false, nil
		}
		return nil, nil, false, err
	}
	m, err = ParseManifest(res.Data)
	if err != nil {
		return nil, nil, false, err
	}
	v := objectstore.UpdateVersion{ETag: res.Meta.ETag, Version: res.Meta.Version}
	return m, &v, true, nil
}

// Create writes the manifest only if it does not already exist (PutCreate).
// Returns objectstore.ErrAlreadyExists if another writer created it first.
func (s *Store) Create(ctx context.Context, m *Manifest) error {
	data, err := m.Bytes()
	if err != nil {
		return err
	}
	return s.os.PutOpts(ctx, s.path, data, objectstore.PutOptions{Mode: objectstore.PutCreate})
}

// Commit writes the manifest only if its stored version still matches
// (PutUpdate). Returns objectstore.ErrPreconditionFailed on a lost CAS race;
// the caller should re-Load and replan.
func (s *Store) Commit(ctx context.Context, m *Manifest, version *objectstore.UpdateVersion) error {
	data, err := m.Bytes()
	if err != nil {
		return err
	}
	if version == nil {
		return s.Create(ctx, m)
	}
	return s.os.PutOpts(ctx, s.path, data, objectstore.PutOptions{
		Mode:    objectstore.PutUpdate,
		Version: *version,
	})
}
