package duckdbq

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
)

// ErrConflict is returned by WatermarkStore.SaveCAS when prev does not match the
// currently stored value (a lost update / concurrent advance).
var ErrConflict = errors.New("duckdbq: watermark CAS conflict")

// WatermarkStore persists an out-of-process Incremental query's progress (how
// far it has returned). SaveCAS does a compare-and-swap on the stored value so
// two runners cannot both advance past the same watermark. The unset state
// compares equal to prev == 0, so the first SaveCAS(0, next) succeeds.
type WatermarkStore interface {
	Load(ctx context.Context) (uint64, bool, error)
	SaveCAS(ctx context.Context, prev, next uint64) error
}

// MemWatermarkStore is an in-process WatermarkStore (a CAS-guarded field).
type MemWatermarkStore struct {
	mu  sync.Mutex
	v   uint64
	set bool
}

// NewMemWatermarkStore returns an unset in-memory watermark store.
func NewMemWatermarkStore() *MemWatermarkStore { return &MemWatermarkStore{} }

// Load implements WatermarkStore.
func (m *MemWatermarkStore) Load(context.Context) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v, m.set, nil
}

// SaveCAS implements WatermarkStore.
func (m *MemWatermarkStore) SaveCAS(_ context.Context, prev, next uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v != prev { // unset compares as 0
		return ErrConflict
	}
	m.v, m.set = next, true
	return nil
}

// FileWatermarkStore persists the watermark as a decimal integer in a file,
// written atomically (temp + rename). The CAS is read-compare-write; it is
// adequate for the single-runner-per-stream contract but is not a cross-process
// atomic CAS. For multi-writer out-of-process coordination, back the watermark
// with an objectstore conditional PUT (If-Match) instead.
type FileWatermarkStore struct {
	path string
	mu   sync.Mutex
}

// NewFileWatermarkStore returns a file-backed watermark store at path.
func NewFileWatermarkStore(path string) *FileWatermarkStore {
	return &FileWatermarkStore{path: path}
}

// Load implements WatermarkStore.
func (f *FileWatermarkStore) Load(context.Context) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.load()
}

func (f *FileWatermarkStore) load() (uint64, bool, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

// SaveCAS implements WatermarkStore.
func (f *FileWatermarkStore) SaveCAS(_ context.Context, prev, next uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, _, err := f.load() // unset reads as 0
	if err != nil {
		return err
	}
	if cur != prev {
		return ErrConflict
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(next, 10)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
