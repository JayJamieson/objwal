package wal

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// CursorStore persists a replica's next-to-apply cursor so a restart resumes
// instead of replaying from the beginning. The cursor is saved after each
// applied segment; because Apply is idempotent, a crash between apply and save
// at worst re-applies one segment.
//
// (A future v2 retention policy could publish cursors to the object store so GC
// can keep min(cursor) across replicas; that is a different use of the same
// idea and is not built here.)
type CursorStore interface {
	// Load returns the persisted next-to-apply cursor. ok is false if nothing
	// has been persisted yet.
	Load(ctx context.Context) (next uint64, ok bool, err error)
	// Save persists the next-to-apply cursor durably.
	Save(ctx context.Context, next uint64) error
}

// MemCursorStore is an in-memory CursorStore for tests.
type MemCursorStore struct {
	mu  sync.Mutex
	val uint64
	set bool
}

func (m *MemCursorStore) Load(context.Context) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.val, m.set, nil
}

func (m *MemCursorStore) Save(_ context.Context, next uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.val, m.set = next, true
	return nil
}

// FileCursorStore persists the cursor to a local file as 8 little-endian bytes,
// written atomically via a temp file and rename. Suitable for a full local
// replica that keeps its cursor next to its data.
type FileCursorStore struct {
	path string
}

func NewFileCursorStore(path string) *FileCursorStore { return &FileCursorStore{path: path} }

func (f *FileCursorStore) Load(context.Context) (uint64, bool, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("wal: cursor load: %w", err)
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("wal: cursor file %s has %d bytes, want 8", f.path, len(data))
	}
	return binary.LittleEndian.Uint64(data), true, nil
}

func (f *FileCursorStore) Save(_ context.Context, next uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], next)
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, buf[:], 0o644); err != nil {
		return fmt.Errorf("wal: cursor save (tmp): %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("wal: cursor save (rename): %w", err)
	}
	return nil
}
