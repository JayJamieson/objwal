package objectstore

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// Rng is SplitMix64: small, seedable, deterministic. Same seed, same sequence.
type Rng struct{ s uint64 }

func NewRng(seed uint64) *Rng { return &Rng{s: seed} }

func (r *Rng) Next() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *Rng) Float() float64 { return float64(r.Next()>>11) / float64(uint64(1)<<53) }

func (r *Rng) Chance(p float64) bool { return p > 0 && r.Float() < p }

// ErrInjected is the synthetic transport failure SimStore returns. It is
// deliberately NOT ErrPreconditionFailed / ErrAlreadyExists / ErrNotFound, so
// callers must treat it as "unknown outcome".
var ErrInjected = errors.New("objectstore: injected fault")

// Faults are per-operation injection probabilities.
type Faults struct {
	// FailClean: the op reports an error and nothing happened.
	FailClean float64
	// FailAmbiguous (mutations only): the op EXECUTES, then reports an
	// error - the S3 "timeout after the write landed" case.
	FailAmbiguous float64
	// KeySubstring, if non-empty, restricts injection to keys containing it.
	KeySubstring string
}

// SimStats counts what passed through the store.
type SimStats struct {
	Gets, Puts, CASFailures, Deletes int
	CleanFaults, AmbiguousFaults     int
}

// SimStore wraps any ObjectStore with seeded fault injection and counters.
// With zero Faults it is a transparent passthrough.
type SimStore struct {
	inner  ObjectStore
	mu     sync.Mutex
	rng    *Rng
	faults Faults
	stats  SimStats
}

func NewSimStore(inner ObjectStore, seed uint64, f Faults) *SimStore {
	return &SimStore{inner: inner, rng: NewRng(seed), faults: f}
}

func (s *SimStore) Stats() SimStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// roll decides the fate of one operation. Serialised so a seed fully
// determines the fault sequence regardless of goroutine interleaving.
func (s *SimStore) roll(path string, mutation bool) (clean, ambiguous bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.KeySubstring != "" && !strings.Contains(path, s.faults.KeySubstring) {
		return false, false
	}
	if s.rng.Chance(s.faults.FailClean) {
		s.stats.CleanFaults++
		return true, false
	}
	if mutation && s.rng.Chance(s.faults.FailAmbiguous) {
		s.stats.AmbiguousFaults++
		return false, true
	}
	return false, false
}

func (s *SimStore) count(f func(*SimStats)) {
	s.mu.Lock()
	f(&s.stats)
	s.mu.Unlock()
}

func (s *SimStore) Get(ctx context.Context, path string) (GetResult, error) {
	s.count(func(st *SimStats) { st.Gets++ })
	if clean, _ := s.roll(path, false); clean {
		return GetResult{}, ErrInjected
	}
	return s.inner.Get(ctx, path)
}

func (s *SimStore) Put(ctx context.Context, path string, data []byte) error {
	s.count(func(st *SimStats) { st.Puts++ })
	clean, ambiguous := s.roll(path, true)
	if clean {
		return ErrInjected
	}
	err := s.inner.Put(ctx, path, data)
	if err == nil && ambiguous {
		return ErrInjected
	}
	return err
}

func (s *SimStore) PutOpts(ctx context.Context, path string, data []byte, opts PutOptions) error {
	s.count(func(st *SimStats) { st.Puts++ })
	clean, ambiguous := s.roll(path, true)
	if clean {
		return ErrInjected
	}
	err := s.inner.PutOpts(ctx, path, data, opts)
	if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrAlreadyExists) {
		s.count(func(st *SimStats) { st.CASFailures++ })
	}
	// The load-bearing case: the write LANDED, then the response was lost.
	if err == nil && ambiguous {
		return ErrInjected
	}
	return err
}

func (s *SimStore) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	if clean, _ := s.roll(prefix, false); clean {
		return nil, ErrInjected
	}
	return s.inner.List(ctx, prefix)
}

func (s *SimStore) Delete(ctx context.Context, path string) error {
	s.count(func(st *SimStats) { st.Deletes++ })
	if clean, _ := s.roll(path, true); clean {
		return ErrInjected
	}
	return s.inner.Delete(ctx, path)
}

var _ ObjectStore = (*SimStore)(nil)
