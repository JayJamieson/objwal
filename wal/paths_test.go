package wal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// recordingApplier collects applied records and can simulate transient apply
// failures to exercise the re-delivery (idempotency) path.
type recordingApplier struct {
	mu       sync.Mutex
	applied  []Record
	failAt   int // fail the (failAt)th apply once, if > 0
	failures int
}

func (a *recordingApplier) Apply(_ context.Context, rec Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failAt > 0 && len(a.applied)+a.failures+1 == a.failAt && a.failures == 0 {
		a.failures++
		return fmt.Errorf("simulated apply failure")
	}
	a.applied = append(a.applied, rec)
	return nil
}

func (a *recordingApplier) datas() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.applied))
	for i, r := range a.applied {
		out[i] = string(r.Data)
	}
	return out
}

const (
	testManifest = "wal/manifest"
	testPrefix   = "wal/segments"
)

func newProducer(t *testing.T, os objectstore.ObjectStore) *Producer {
	t.Helper()
	p, err := NewProducer(context.Background(), os, ProducerConfig{
		ManifestPath:  testManifest,
		SegmentPrefix: testPrefix,
		FlushInterval: 5 * time.Millisecond,
		Compression:   CompressionNone,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

func TestProduceThenTail(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	defer p.Close(ctx)

	want := []string{"put:a=1", "put:b=2", "del:a"}
	for _, w := range want {
		d, err := p.Append(ctx, [][]byte{[]byte(w)}, []byte("grp"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatalf("durability wait: %v", err)
		}
	}

	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest})
	n, err := r.Poll(ctx)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n != len(want) {
		t.Fatalf("applied %d, want %d", n, len(want))
	}
	got := app.datas()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
	// Cursor advanced to the last sequence; a second poll applies nothing.
	if again, _ := r.Poll(ctx); again != 0 {
		t.Fatalf("second poll applied %d, want 0", again)
	}
}

func TestGroupCommitCoalescesAndPreservesMeta(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	// Large flush interval + size trigger off => everything before Close
	// coalesces into a single segment, exercising multi-group metadata.
	p, err := NewProducer(ctx, os, ProducerConfig{
		ManifestPath:  testManifest,
		SegmentPrefix: testPrefix,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	d1, _ := p.Append(ctx, [][]byte{[]byte("r0"), []byte("r1")}, []byte("groupA"))
	d2, _ := p.Append(ctx, [][]byte{[]byte("r2")}, []byte("groupB"))
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s1, err := d1.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := d2.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s2 {
		t.Fatalf("groups in one segment should share a sequence: %d vs %d", s1, s2)
	}

	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.applied) != 3 {
		t.Fatalf("applied %d records, want 3", len(app.applied))
	}
	// r0,r1 carry groupA; r2 carries groupB.
	if string(app.applied[0].GroupMeta) != "groupA" || string(app.applied[1].GroupMeta) != "groupA" {
		t.Fatalf("group A metadata wrong: %q %q", app.applied[0].GroupMeta, app.applied[1].GroupMeta)
	}
	if string(app.applied[2].GroupMeta) != "groupB" {
		t.Fatalf("group B metadata wrong: %q", app.applied[2].GroupMeta)
	}
}

func TestEpochFencingOnFailover(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()

	primaryA := newProducer(t, os)
	if primaryA.Epoch() != 1 {
		t.Fatalf("first producer epoch = %d, want 1", primaryA.Epoch())
	}
	d, _ := primaryA.Append(ctx, [][]byte{[]byte("from-A")}, nil)
	if _, err := d.Wait(ctx); err != nil {
		t.Fatalf("A's first write should succeed: %v", err)
	}

	// A new primary takes over: claims a higher epoch.
	primaryB := newProducer(t, os)
	if primaryB.Epoch() != 2 {
		t.Fatalf("second producer epoch = %d, want 2", primaryB.Epoch())
	}

	// The stale primary A is now fenced: its next write must fail with ErrFenced.
	dA, _ := primaryA.Append(ctx, [][]byte{[]byte("from-A-zombie")}, nil)
	if _, err := dA.Wait(ctx); err == nil {
		t.Fatal("expected fenced producer A write to fail")
	} else if err != ErrFenced && !isWrapped(err, ErrFenced) {
		t.Fatalf("expected ErrFenced, got %v", err)
	}

	// B continues to write successfully.
	dB, _ := primaryB.Append(ctx, [][]byte{[]byte("from-B")}, nil)
	if _, err := dB.Wait(ctx); err != nil {
		t.Fatalf("B's write should succeed: %v", err)
	}
	primaryB.Close(ctx)

	// The replica sees A's first write and B's write, but never the zombie.
	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	got := app.datas()
	want := []string{"from-A", "from-B"}
	if len(got) != len(want) {
		t.Fatalf("applied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestApplyFailureCausesIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	for _, w := range []string{"x", "y", "z"} {
		d, _ := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	p.Close(ctx)

	// Fail on the 2nd apply once; the cursor must not have advanced past the
	// failing segment, so a re-poll re-delivers it.
	app := &recordingApplier{failAt: 2}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err == nil {
		t.Fatal("expected first poll to surface the apply failure")
	}
	cursorAfterFail := r.Next()
	if _, err := r.Poll(ctx); err != nil {
		t.Fatalf("second poll should succeed: %v", err)
	}
	if r.Next() <= cursorAfterFail && cursorAfterFail != 0 {
		// cursor should now be at the final sequence
	}
	// All three eventually applied, in order, with the failed one re-delivered.
	got := app.datas()
	want := []string{"x", "y", "z"}
	if len(got) != len(want) {
		t.Fatalf("applied %v, want %v (re-delivery expected)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func isWrapped(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
