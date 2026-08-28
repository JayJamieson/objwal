package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSimStore_CorruptFlipsOneByteWithoutTouchingTheStore(t *testing.T) {
	inner := NewInMemory()
	ctx := context.Background()
	orig := []byte("hello world")
	if err := inner.Put(ctx, "k", orig); err != nil {
		t.Fatal(err)
	}

	sim := NewSimStore(inner, 1, Faults{Corrupt: 1.0})
	res, err := sim.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(res.Data) == string(orig) {
		t.Fatal("Corrupt: 1.0 but returned data matches the original exactly")
	}
	diff := 0
	for i := range res.Data {
		if res.Data[i] != orig[i] {
			diff++
		}
	}
	if diff != 1 {
		t.Fatalf("expected exactly one flipped byte, got %d differing bytes", diff)
	}

	// The store itself must be untouched: a second Get through the inner
	// store (bypassing SimStore) sees the original bytes.
	direct, err := inner.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(direct.Data) != string(orig) {
		t.Fatalf("corruption leaked into the backing store: got %q, want %q", direct.Data, orig)
	}
	if got := sim.Stats().CorruptReads; got != 1 {
		t.Fatalf("CorruptReads = %d, want 1", got)
	}
}

func TestSimStore_CorruptOnlyAppliesToReads(t *testing.T) {
	inner := NewInMemory()
	ctx := context.Background()
	sim := NewSimStore(inner, 1, Faults{Corrupt: 1.0})
	if err := sim.Put(ctx, "k", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	direct, err := inner.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(direct.Data) != "payload" {
		t.Fatalf("Corrupt fired on a write: stored %q, want %q", direct.Data, "payload")
	}
}

func TestSimStore_SlowAmbiguousWriteLandsButReportsInjectedAfterDelay(t *testing.T) {
	inner := NewInMemory()
	ctx := context.Background()
	sim := NewSimStore(inner, 1, Faults{SlowAmbiguous: 1.0, SlowDelay: 20 * time.Millisecond})

	start := time.Now()
	err := sim.Put(ctx, "k", []byte("payload"))
	elapsed := time.Since(start)

	if !errors.Is(err, ErrInjected) {
		t.Fatalf("err = %v, want ErrInjected", err)
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("returned after %v, want at least ~SlowDelay (20ms)", elapsed)
	}
	// The write executed before the delay: it landed regardless of the
	// fault reported afterward.
	res, gerr := inner.Get(ctx, "k")
	if gerr != nil {
		t.Fatalf("SlowAmbiguous reported failure but the write did not land: %v", gerr)
	}
	if string(res.Data) != "payload" {
		t.Fatalf("landed data = %q, want %q", res.Data, "payload")
	}
	if got := sim.Stats().SlowFaults; got != 1 {
		t.Fatalf("SlowFaults = %d, want 1", got)
	}
}

func TestSimStore_SlowAmbiguousRespectsCallerContext(t *testing.T) {
	inner := NewInMemory()
	sim := NewSimStore(inner, 1, Faults{SlowAmbiguous: 1.0, SlowDelay: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := sim.Put(ctx, "k", []byte("payload"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded (caller gave up before the 1h SlowDelay)", err)
	}
	// The write still executed synchronously before the (abandoned) delay.
	res, gerr := inner.Get(context.Background(), "k")
	if gerr != nil {
		t.Fatalf("write did not land despite executing before the delay: %v", gerr)
	}
	if string(res.Data) != "payload" {
		t.Fatalf("landed data = %q, want %q", res.Data, "payload")
	}
}

func TestSimStore_LatencyDelaysAndPassesThrough(t *testing.T) {
	inner := NewInMemory()
	ctx := context.Background()
	if err := inner.Put(ctx, "k", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	sim := NewSimStore(inner, 1, Faults{Latency: 20 * time.Millisecond})

	start := time.Now()
	res, err := sim.Get(ctx, "k")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(res.Data) != "payload" {
		t.Fatalf("data = %q, want %q", res.Data, "payload")
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("returned after %v, want at least ~Latency (20ms)", elapsed)
	}
}

func TestSimStore_LatencyRespectsCallerContext(t *testing.T) {
	inner := NewInMemory()
	sim := NewSimStore(inner, 1, Faults{Latency: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := sim.Get(ctx, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if got := sim.Stats().Gets; got != 0 {
		t.Fatalf("Gets = %d, want 0: a call abandoned during injected latency never reached the store", got)
	}
}

// TestSimStore_ZeroFaultsIsDeterministicPassthrough pins that a SimStore with
// the new fault fields left at their zero value behaves exactly like one that
// only sets FailClean/FailAmbiguous: the added rng.Chance checks for
// SlowAmbiguous/Corrupt must never consume randomness when their probability
// is 0, or every seed used by the existing DST suites would silently reorder.
func TestSimStore_ZeroFaultsIsDeterministicPassthrough(t *testing.T) {
	ctx := context.Background()
	run := func(f Faults) (results []error) {
		inner := NewInMemory()
		sim := NewSimStore(inner, 42, f)
		for range 50 {
			results = append(results, sim.Put(ctx, "k", []byte("v")))
		}
		return results
	}
	before := run(Faults{FailAmbiguous: 0.3, FailClean: 0.2})
	after := run(Faults{FailAmbiguous: 0.3, FailClean: 0.2, SlowAmbiguous: 0, Corrupt: 0})
	if len(before) != len(after) {
		t.Fatalf("length mismatch: %d vs %d", len(before), len(after))
	}
	for i := range before {
		if (before[i] == nil) != (after[i] == nil) {
			t.Fatalf("call %d: fault sequence diverged with zero-valued new fields present", i)
		}
	}
}
