package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// --- Applier: a tiny EXTERNAL kv "engine" the wal package knows nothing
// about, driven through TypedApplier's decode + handle functions. ---

type kvKind uint8

const (
	opPut kvKind = iota
	opDelete
)

type kvOp struct {
	Kind  kvKind
	Key   string
	Value string
}

// encodeKVOp is the writer-side frame; decodeKVOp is its inverse. Both are
// application-owned; the wal layer never sees this format.
func encodeKVOp(op kvOp) []byte {
	var b []byte
	b = append(b, byte(op.Kind))
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(op.Key)))
	b = append(b, n[:]...)
	b = append(b, op.Key...)
	b = append(b, op.Value...)
	return b
}

func decodeKVOp(data []byte) (kvOp, error) {
	if len(data) < 5 {
		return kvOp{}, fmt.Errorf("short kv record")
	}
	kind := kvKind(data[0])
	klen := int(binary.LittleEndian.Uint32(data[1:5]))
	if 5+klen > len(data) {
		return kvOp{}, fmt.Errorf("key overruns record")
	}
	return kvOp{Kind: kind, Key: string(data[5 : 5+klen]), Value: string(data[5+klen:])}, nil
}

// fakeKV stands in for Bitcask: an external engine with its own API.
type fakeKV struct {
	mu sync.Mutex
	m  map[string]string
}

func newFakeKV() *fakeKV { return &fakeKV{m: map[string]string{}} }
func (k *fakeKV) Put(key, val string) error {
	k.mu.Lock()
	k.m[key] = val
	k.mu.Unlock()
	return nil
}
func (k *fakeKV) Delete(key string) error {
	k.mu.Lock()
	delete(k.m, key)
	k.mu.Unlock()
	return nil
}
func (k *fakeKV) get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	return v, ok
}

func TestTypedApplierDrivesExternalEngine(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)

	ops := []kvOp{
		{Kind: opPut, Key: "a", Value: "1"},
		{Kind: opPut, Key: "b", Value: "2"},
		{Kind: opPut, Key: "a", Value: "3"}, // overwrite
		{Kind: opDelete, Key: "b"},
	}
	for _, op := range ops {
		d, err := p.Append(ctx, [][]byte{encodeKVOp(op)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	// The engine (fakeKV) is entirely external; wal only runs decode + handle.
	db := newFakeKV()
	applier := TypedApplier(decodeKVOp, func(_ context.Context, _ uint64, op kvOp) error {
		switch op.Kind {
		case opPut:
			return db.Put(op.Key, op.Value)
		case opDelete:
			return db.Delete(op.Key)
		}
		return fmt.Errorf("unknown op")
	})

	r := NewReplica(os, applier, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if v, ok := db.get("a"); !ok || v != "3" {
		t.Fatalf("a = %q,%v, want 3,true", v, ok)
	}
	if _, ok := db.get("b"); ok {
		t.Fatal("b should have been deleted")
	}

	// Idempotent re-delivery: replaying the whole log lands the same state.
	r2 := NewReplica(os, applier, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r2.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.get("a"); v != "3" {
		t.Fatalf("after replay a = %q, want 3", v)
	}
}

func TestApplyFuncAdapter(t *testing.T) {
	var got []byte
	var a Applier = ApplyFunc(func(_ context.Context, rec Record) error {
		got = rec.Data
		return nil
	})
	if err := a.Apply(context.Background(), Record{Data: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Fatalf("adapter passed %q", got)
	}
}

// --- Cursor persistence / resume ---

func TestFileCursorStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cursor")
	cs := NewFileCursorStore(path)

	if _, ok, err := cs.Load(ctx); err != nil || ok {
		t.Fatalf("fresh load: ok=%v err=%v", ok, err)
	}
	if err := cs.Save(ctx, 42); err != nil {
		t.Fatal(err)
	}
	got, ok, err := cs.Load(ctx)
	if err != nil || !ok || got != 42 {
		t.Fatalf("load = %d,%v,%v want 42,true,nil", got, ok, err)
	}
}

func TestReplicaResumesFromPersistedCursor(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	// Four single-record segments => sequences 0..3.
	for _, w := range []string{"s0", "s1", "s2", "s3"} {
		d, _ := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	cursorPath := filepath.Join(t.TempDir(), "cursor")
	cs := NewFileCursorStore(cursorPath)

	// Replica 1 fails applying seq 2, so it persists cursor through seq 1 only.
	app1 := &recordingApplier{failAt: 3} // 3rd apply overall = seq 2
	r1 := NewReplica(os, app1, ReplicaConfig{ManifestPath: testManifest, Cursor: cs})
	if _, err := r1.Poll(ctx); err == nil {
		t.Fatal("expected r1 to fail on seq 2")
	}
	if next, _, _ := cs.Load(ctx); next != 2 {
		t.Fatalf("persisted cursor = %d, want 2 (seqs 0,1 applied)", next)
	}

	// Replica 2 (fresh instance) resumes from the persisted cursor and applies
	// only the remaining segments.
	app2 := &recordingApplier{}
	r2 := NewReplica(os, app2, ReplicaConfig{ManifestPath: testManifest, Cursor: cs})
	n, err := r2.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("r2 applied %d, want 2 (resumed at seq 2)", n)
	}
	if got := app2.datas(); len(got) != 2 || got[0] != "s2" || got[1] != "s3" {
		t.Fatalf("r2 applied %v, want [s2 s3]", got)
	}
	if r2.Next() != 4 {
		t.Fatalf("r2.Next() = %d, want 4", r2.Next())
	}
}

// TestReplicaMaxRecordsPerPoll bounds ingestion: each Poll applies at most the
// configured budget, stopping at a segment boundary and resuming on the next
// Poll, so a fully-committed WAL is drained gradually across polls.
func TestReplicaMaxRecordsPerPoll(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	// Five single-record segments => sequences 0..4.
	for _, w := range []string{"s0", "s1", "s2", "s3", "s4"} {
		d, _ := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest, MaxRecordsPerPoll: 2})

	// Budget 2 over 1-record segments: 2, 2, 1, then 0.
	for _, want := range []struct{ applied, next int }{{2, 2}, {2, 4}, {1, 5}, {0, 5}} {
		n, err := r.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n != want.applied {
			t.Fatalf("poll applied %d, want %d", n, want.applied)
		}
		if r.Next() != uint64(want.next) {
			t.Fatalf("after poll Next() = %d, want %d", r.Next(), want.next)
		}
	}
	if got := app.datas(); len(got) != 5 || got[0] != "s0" || got[4] != "s4" {
		t.Fatalf("applied %v, want [s0..s4] in order", got)
	}
}

// --- Upload retry / backoff ---

// flakyStore wraps an ObjectStore and fails the first failPuts unconditional
// Puts (segment uploads) with a transient error; PutOpts (manifest CAS) is
// always delegated.
type flakyStore struct {
	objectstore.ObjectStore
	mu       sync.Mutex
	failPuts int
	seen     int
}

func (f *flakyStore) Put(ctx context.Context, path string, data []byte) error {
	f.mu.Lock()
	f.seen++
	fail := f.seen <= f.failPuts
	f.mu.Unlock()
	if fail {
		return errors.New("simulated transient upload error")
	}
	return f.ObjectStore.Put(ctx, path, data)
}

func newFlakyProducer(t *testing.T, os objectstore.ObjectStore, maxAttempts int) *Producer {
	t.Helper()
	p, err := NewProducer(context.Background(), os, ProducerConfig{
		ManifestPath:         testManifest,
		SegmentPrefix:        testPrefix,
		FlushInterval:        5 * time.Millisecond,
		UploadMaxAttempts:    maxAttempts,
		UploadInitialBackoff: time.Millisecond, // keep tests fast
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

func TestSegmentUploadRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	flaky := &flakyStore{ObjectStore: objectstore.NewInMemory(), failPuts: 2}
	p := newFlakyProducer(t, flaky, 6)
	defer func() { _ = p.Close(ctx) }()

	d, err := p.Append(ctx, [][]byte{[]byte("survives-flakes")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err != nil {
		t.Fatalf("append should succeed after retries: %v", err)
	}

	app := &recordingApplier{}
	r := NewReplica(flaky, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := app.datas(); len(got) != 1 || got[0] != "survives-flakes" {
		t.Fatalf("applied %v, want [survives-flakes]", got)
	}
}

func TestSegmentUploadRetryExhausts(t *testing.T) {
	ctx := context.Background()
	// Fail more times than the attempt budget => the batch fails permanently.
	flaky := &flakyStore{ObjectStore: objectstore.NewInMemory(), failPuts: 100}
	p := newFlakyProducer(t, flaky, 3)
	defer func() { _ = p.Close(ctx) }()

	d, err := p.Append(ctx, [][]byte{[]byte("never-lands")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err == nil {
		t.Fatal("expected append to fail after exhausting upload attempts")
	}

	// Nothing was committed to the manifest, so a replica applies nothing.
	app := &recordingApplier{}
	r := NewReplica(flaky, app, ReplicaConfig{ManifestPath: testManifest})
	if n, err := r.Poll(ctx); err != nil || n != 0 {
		t.Fatalf("replica saw %d records (err=%v), want 0", n, err)
	}
}
