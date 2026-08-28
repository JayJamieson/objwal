package objectstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

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

// defaultSlowDelay is used when SlowAmbiguous > 0 but SlowDelay is unset.
const defaultSlowDelay = 50 * time.Millisecond

// Faults are per-operation injection probabilities.
type Faults struct {
	// FailClean: the op reports an error and nothing happened.
	FailClean float64
	// FailAmbiguous (mutations only): the op EXECUTES, then immediately
	// reports an error - the S3 "response lost right after the write landed"
	// case.
	FailAmbiguous float64
	// SlowAmbiguous (mutations only): the op EXECUTES, then the call blocks
	// for SlowDelay before reporting an error - the write landed, but the
	// response was slow enough that the caller gave up waiting on it. If the
	// caller's context expires first, ctx.Err() is returned instead; either
	// way the write already landed, so this is ambiguity WITH real elapsed
	// time, distinct from FailAmbiguous's instant report.
	SlowAmbiguous float64
	// SlowDelay bounds how long a SlowAmbiguous op blocks before reporting
	// the fault. Defaults to 50ms when SlowAmbiguous > 0 and SlowDelay == 0.
	SlowDelay time.Duration
	// Corrupt (reads only): the op succeeds but one byte of the returned data
	// is flipped, simulating a bit error in transit. Rolled independently per
	// call, so a retry is not guaranteed to see the same corruption - this
	// models an in-flight fault, not a permanently corrupt stored object.
	Corrupt float64
	// Latency/LatencyJitter, if Latency is positive, delay every op (faulted
	// or not) by a duration drawn uniformly from
	// [Latency-LatencyJitter, Latency+LatencyJitter], modeling ordinary
	// network/store latency rather than a failure. The delay is ctx-aware: a
	// context that expires first yields ctx.Err() and the op is not
	// attempted.
	Latency, LatencyJitter time.Duration
	// KeySubstring, if non-empty, restricts injection to keys containing it.
	KeySubstring string
}

// SimStats counts what passed through the store.
type SimStats struct {
	Gets, Puts, CASFailures, Deletes int
	CleanFaults, AmbiguousFaults     int
	SlowFaults, CorruptReads         int
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
func (s *SimStore) roll(path string, mutation bool) (clean, ambiguous, slow, corrupt bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.KeySubstring != "" && !strings.Contains(path, s.faults.KeySubstring) {
		return false, false, false, false
	}
	if s.rng.Chance(s.faults.FailClean) {
		s.stats.CleanFaults++
		return true, false, false, false
	}
	if mutation {
		if s.rng.Chance(s.faults.FailAmbiguous) {
			s.stats.AmbiguousFaults++
			return false, true, false, false
		}
		if s.rng.Chance(s.faults.SlowAmbiguous) {
			s.stats.SlowFaults++
			return false, false, true, false
		}
	} else if s.rng.Chance(s.faults.Corrupt) {
		s.stats.CorruptReads++
		return false, false, false, true
	}
	return false, false, false, false
}

// jitteredLatency draws this call's injected delay, or 0 if Latency is unset.
func (s *SimStore) jitteredLatency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.Latency <= 0 {
		return 0
	}
	if s.faults.LatencyJitter <= 0 {
		return s.faults.Latency
	}
	span := 2*int64(s.faults.LatencyJitter) + 1
	offset := int64(s.rng.Next()%uint64(span)) - int64(s.faults.LatencyJitter)
	d := s.faults.Latency + time.Duration(offset)
	if d < 0 {
		return 0
	}
	return d
}

func (s *SimStore) slowDelay() time.Duration {
	if s.faults.SlowDelay > 0 {
		return s.faults.SlowDelay
	}
	return defaultSlowDelay
}

// sleep blocks for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flipByte returns a copy of data with one byte inverted, deterministically
// chosen from the RNG stream.
func (s *SimStore) flipByte(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	s.mu.Lock()
	i := int(s.rng.Next() % uint64(len(data)))
	s.mu.Unlock()
	out := append([]byte(nil), data...)
	out[i] ^= 0xFF
	return out
}

func (s *SimStore) count(f func(*SimStats)) {
	s.mu.Lock()
	f(&s.stats)
	s.mu.Unlock()
}

func (s *SimStore) Get(ctx context.Context, path string) (GetResult, error) {
	if err := sleep(ctx, s.jitteredLatency()); err != nil {
		return GetResult{}, err
	}
	s.count(func(st *SimStats) { st.Gets++ })
	clean, _, _, corrupt := s.roll(path, false)
	if clean {
		return GetResult{}, ErrInjected
	}
	res, err := s.inner.Get(ctx, path)
	if err == nil && corrupt {
		res.Data = s.flipByte(res.Data)
	}
	return res, err
}

func (s *SimStore) Put(ctx context.Context, path string, data []byte) error {
	if err := sleep(ctx, s.jitteredLatency()); err != nil {
		return err
	}
	s.count(func(st *SimStats) { st.Puts++ })
	clean, ambiguous, slow, _ := s.roll(path, true)
	if clean {
		return ErrInjected
	}
	err := s.inner.Put(ctx, path, data)
	if err == nil && (ambiguous || slow) {
		if slow {
			if serr := sleep(ctx, s.slowDelay()); serr != nil {
				return serr
			}
		}
		return ErrInjected
	}
	return err
}

func (s *SimStore) PutOpts(ctx context.Context, path string, data []byte, opts PutOptions) error {
	if err := sleep(ctx, s.jitteredLatency()); err != nil {
		return err
	}
	s.count(func(st *SimStats) { st.Puts++ })
	clean, ambiguous, slow, _ := s.roll(path, true)
	if clean {
		return ErrInjected
	}
	err := s.inner.PutOpts(ctx, path, data, opts)
	if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrAlreadyExists) {
		s.count(func(st *SimStats) { st.CASFailures++ })
	}
	// The load-bearing case: the write LANDED, then the response was lost
	// (immediately for ambiguous, after a real delay for slow).
	if err == nil && (ambiguous || slow) {
		if slow {
			if serr := sleep(ctx, s.slowDelay()); serr != nil {
				return serr
			}
		}
		return ErrInjected
	}
	return err
}

func (s *SimStore) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	if err := sleep(ctx, s.jitteredLatency()); err != nil {
		return nil, err
	}
	if clean, _, _, _ := s.roll(prefix, false); clean {
		return nil, ErrInjected
	}
	return s.inner.List(ctx, prefix)
}

func (s *SimStore) Delete(ctx context.Context, path string) error {
	if err := sleep(ctx, s.jitteredLatency()); err != nil {
		return err
	}
	s.count(func(st *SimStats) { st.Deletes++ })
	clean, ambiguous, slow, _ := s.roll(path, true)
	if clean {
		return ErrInjected
	}
	err := s.inner.Delete(ctx, path)
	if err == nil && (ambiguous || slow) {
		if slow {
			if serr := sleep(ctx, s.slowDelay()); serr != nil {
				return serr
			}
		}
		return ErrInjected
	}
	return err
}

var _ ObjectStore = (*SimStore)(nil)
