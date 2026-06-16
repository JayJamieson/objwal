package wal

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// concurrencyStore records the maximum number of Puts (segment uploads) in
// flight at once, and can hold all Puts at a barrier so overlap is observable.
type concurrencyStore struct {
	objectstore.ObjectStore
	inFlight int32
	maxSeen  int32
	hold     chan struct{} // Puts block on this until closed (nil = no hold)
}

func (c *concurrencyStore) Put(ctx context.Context, path string, data []byte) error {
	n := atomic.AddInt32(&c.inFlight, 1)
	for {
		old := atomic.LoadInt32(&c.maxSeen)
		if n <= old || atomic.CompareAndSwapInt32(&c.maxSeen, old, n) {
			break
		}
	}
	if c.hold != nil {
		<-c.hold
	}
	atomic.AddInt32(&c.inFlight, -1)
	return c.ObjectStore.Put(ctx, path, data)
}

func TestParallelUploadsOverlap(t *testing.T) {
	ctx := context.Background()
	cs := &concurrencyStore{ObjectStore: objectstore.NewInMemory(), hold: make(chan struct{})}
	p, err := NewProducer(ctx, cs, ProducerConfig{
		ManifestPath:      testManifest,
		SegmentPrefix:     testPrefix,
		FlushInterval:     time.Hour, // single flush at Close
		SegmentMaxBytes:   4,         // one 4-byte record per segment
		UploadConcurrency: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	var last *Durability
	for _, w := range []string{"aaaa", "bbbb", "cccc", "dddd"} {
		d, err := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		last = d
	}

	// Release the upload barrier shortly after Close starts its flush, so the
	// four uploads pile up against it and overlap.
	go func() {
		time.Sleep(40 * time.Millisecond)
		close(cs.hold)
	}()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := last.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&cs.maxSeen); got < 2 {
		t.Fatalf("max concurrent uploads = %d, want >= 2 (uploads should overlap)", got)
	}
}

// reorderStore makes lower-ordinal segment uploads finish LATER than higher
// ones, so upload completion order is the reverse of ordinal order. The
// committed log must still be in ordinal order.
type reorderStore struct {
	objectstore.ObjectStore
	unit time.Duration
}

func (r *reorderStore) Put(ctx context.Context, path string, data []byte) error {
	ord := ordinalFromPathNoT(path)
	// Higher ordinal => shorter delay => completes first.
	time.Sleep(time.Duration(8-min64(ord, 8)) * r.unit)
	return r.ObjectStore.Put(ctx, path, data)
}

func ordinalFromPathNoT(path string) uint64 {
	parts := strings.Split(path, "/")
	v, _ := strconv.ParseUint(parts[len(parts)-1], 16, 64)
	return v
}
func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func TestParallelUploadsCommitInOrderDespiteReorderedCompletion(t *testing.T) {
	ctx := context.Background()
	rs := &reorderStore{ObjectStore: objectstore.NewInMemory(), unit: 15 * time.Millisecond}
	p, err := NewProducer(ctx, rs, ProducerConfig{
		ManifestPath:      testManifest,
		SegmentPrefix:     testPrefix,
		FlushInterval:     time.Hour,
		SegmentMaxBytes:   4, // one record per segment => distinct ordinals
		UploadConcurrency: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"r000", "r001", "r002", "r003", "r004"}
	durs := make([]*Durability, len(want))
	for i, w := range want {
		d, err := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		durs[i] = d
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Sequences must be assigned in ordinal (= Append) order, even though the
	// uploads completed in reverse.
	for i, d := range durs {
		seq, err := d.Wait(ctx)
		if err != nil {
			t.Fatalf("durability %d: %v", i, err)
		}
		if seq != uint64(i) {
			t.Fatalf("record %d (%q) got sequence %d, want %d — ordering violated", i, want[i], seq, i)
		}
	}

	// And the replica observes them in order.
	app := &recordingApplier{}
	rep := NewReplica(rs, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := rep.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	got := app.datas()
	if len(got) != len(want) {
		t.Fatalf("applied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d = %q, want %q (out of order)", i, got[i], want[i])
		}
	}
}

// failOrdinalStore fails uploads for a specific ordinal (always), so retries
// exhaust; all other uploads succeed.
type failOrdinalStore struct {
	objectstore.ObjectStore
	failOrd uint64
}

func (f *failOrdinalStore) Put(ctx context.Context, path string, data []byte) error {
	if ordinalFromPathNoT(path) == f.failOrd {
		return errors.New("simulated permanent upload failure")
	}
	return f.ObjectStore.Put(ctx, path, data)
}

// The key correctness test: if an earlier segment's upload fails, no later
// segment may commit ahead of it. The committed log is a strict prefix; the
// failed segment and everything after it fail, and the producer halts.
func TestUploadFailureHaltsAndPreservesPrefix(t *testing.T) {
	ctx := context.Background()
	fs := &failOrdinalStore{ObjectStore: objectstore.NewInMemory(), failOrd: 1}
	p, err := NewProducer(ctx, fs, ProducerConfig{
		ManifestPath:         testManifest,
		SegmentPrefix:        testPrefix,
		FlushInterval:        time.Hour,
		SegmentMaxBytes:      4, // one record per segment => ordinals 0,1,2
		UploadConcurrency:    4,
		UploadMaxAttempts:    2,
		UploadInitialBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	d0, _ := p.Append(ctx, [][]byte{[]byte("keep")}, nil) // ordinal 0, succeeds
	d1, _ := p.Append(ctx, [][]byte{[]byte("fail")}, nil) // ordinal 1, fails upload
	d2, _ := p.Append(ctx, [][]byte{[]byte("drop")}, nil) // ordinal 2, must NOT commit ahead of 1
	_ = p.Close(ctx)                                      // flush halts; Close returns the halt error

	if seq, err := d0.Wait(ctx); err != nil || seq != 0 {
		t.Fatalf("d0 should commit at seq 0: seq=%d err=%v", seq, err)
	}
	if _, err := d1.Wait(ctx); err == nil {
		t.Fatal("d1 (failed upload) must not be reported durable")
	}
	if _, err := d2.Wait(ctx); err == nil {
		t.Fatal("d2 must NOT commit ahead of the failed earlier segment")
	}

	// Producer is halted: further Appends fail fast.
	if _, err := p.Append(ctx, [][]byte{[]byte("after")}, nil); err == nil {
		t.Fatal("producer should be halted after an upload failure")
	}

	// The replica sees ONLY the committed prefix — never "drop".
	app := &recordingApplier{}
	rep := NewReplica(fs, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := rep.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	got := app.datas()
	if len(got) != 1 || got[0] != "keep" {
		t.Fatalf("replica applied %v, want [keep] only (prefix preserved, no leapfrog)", got)
	}
}

// Directly addresses the "no manifest in S3" question: producing must create a
// single manifest object at ManifestPath (segments live separately under
// SegmentPrefix). Uses the in-memory store; the real S3 path is exercised only
// against a live bucket, not here (see the explanation in chat).
func TestManifestObjectExistsAfterProduce(t *testing.T) {
	ctx := context.Background()
	mem := objectstore.NewInMemory()
	p, err := NewProducer(ctx, mem, ProducerConfig{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/segments",
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		d, err := p.Append(ctx, [][]byte{[]byte("rec")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	// Exactly one manifest object exists, even after the epoch claim plus
	// several commits — it is updated in place, not appended to.
	manifests, err := mem.List(ctx, "wal/manifest")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || manifests[0].Location != "wal/manifest" {
		t.Fatalf("manifest objects = %+v, want exactly one at wal/manifest", manifests)
	}
	// It parses and reflects the committed state.
	got, _, ok, err := NewStore(mem, "wal/manifest").Load(ctx)
	if err != nil || !ok {
		t.Fatalf("manifest load: ok=%v err=%v", ok, err)
	}
	if got.Count() != 3 || got.NextSequence() != 3 {
		t.Fatalf("manifest has count=%d next=%d, want 3/3", got.Count(), got.NextSequence())
	}
	// Segments are separate objects (one per flush), so there are several.
	segs, err := mem.List(ctx, "wal/segments")
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("expected segment objects under wal/segments")
	}
}
