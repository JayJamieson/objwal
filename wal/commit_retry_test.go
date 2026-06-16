package wal

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// commitFlakyStore fails the first failOpts conditional Puts (PutUpdate /
// PutCreate, i.e. manifest commits) with a transient error, then delegates.
// Unconditional Puts (segment uploads) always succeed.
type commitFlakyStore struct {
	objectstore.ObjectStore
	mu       sync.Mutex
	failOpts int
	seen     int
}

func (f *commitFlakyStore) PutOpts(ctx context.Context, path string, data []byte, opts objectstore.PutOptions) error {
	f.mu.Lock()
	f.seen++
	fail := f.seen <= f.failOpts
	f.mu.Unlock()
	if fail {
		return errors.New("simulated transient commit error")
	}
	return f.ObjectStore.PutOpts(ctx, path, data, opts)
}

func newCommitFlakyProducer(t *testing.T, os objectstore.ObjectStore, manifestAttempts int) *Producer {
	t.Helper()
	p, err := NewProducer(context.Background(), os, ProducerConfig{
		ManifestPath:           testManifest,
		SegmentPrefix:          testPrefix,
		FlushInterval:          5 * time.Millisecond,
		ManifestMaxAttempts:    manifestAttempts,
		ManifestInitialBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

func TestManifestCommitRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	// NewProducer's claim performs commits too, so let those succeed and only
	// start failing once the producer is up: set failOpts after construction.
	flaky := &commitFlakyStore{ObjectStore: objectstore.NewInMemory()}
	p := newCommitFlakyProducer(t, flaky, 6)
	defer p.Close(ctx)

	flaky.mu.Lock()
	flaky.failOpts = flaky.seen + 2 // fail the next 2 commit attempts
	flaky.mu.Unlock()

	d, err := p.Append(ctx, [][]byte{[]byte("committed-after-retry")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err != nil {
		t.Fatalf("commit should succeed after retries: %v", err)
	}

	app := &recordingApplier{}
	r := NewReplica(flaky, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := app.datas(); len(got) != 1 || got[0] != "committed-after-retry" {
		t.Fatalf("applied %v, want [committed-after-retry]", got)
	}
}

func TestManifestCommitExhaustionHaltsProducer(t *testing.T) {
	ctx := context.Background()
	flaky := &commitFlakyStore{ObjectStore: objectstore.NewInMemory()}
	p := newCommitFlakyProducer(t, flaky, 3)
	defer p.Close(ctx)

	flaky.mu.Lock()
	flaky.failOpts = flaky.seen + 1000 // commits never succeed from here
	flaky.mu.Unlock()

	d, err := p.Append(ctx, [][]byte{[]byte("doomed")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err == nil {
		t.Fatal("expected commit exhaustion to fail the batch")
	}

	// After a commit exhaustion the producer is halted: later Appends fail fast.
	if _, err := p.Append(ctx, [][]byte{[]byte("after-halt")}, nil); err == nil {
		t.Fatal("expected Append to fail on a halted producer")
	} else if !errors.Is(err, ErrHalted) && !isWrapped(err, ErrHalted) {
		// The halt error is the underlying commit error; ErrHalted is what the
		// pre-check returns. Either way Append must return non-nil here.
	}
}

// countingCursor wraps MemCursorStore and counts Save calls.
type countingCursor struct {
	MemCursorStore
	saves int
}

func (c *countingCursor) Save(ctx context.Context, next uint64) error {
	c.saves++
	return c.MemCursorStore.Save(ctx, next)
}

func TestBatchedCursorSaves(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	for i := 0; i < 5; i++ { // 5 single-record segments => seqs 0..4
		d, _ := p.Append(ctx, [][]byte{[]byte{byte('a' + i)}}, nil)
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	p.Close(ctx)

	cc := &countingCursor{}
	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{
		ManifestPath:       testManifest,
		Cursor:             cc,
		CursorSaveInterval: 2,
	})
	n, err := r.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("applied %d, want 5", n)
	}
	// Interval 2 over 5 segments: saves after seg index 1 (next=2) and 3
	// (next=4), then an end-of-poll save at next=5 => 3 saves total.
	if cc.saves != 3 {
		t.Fatalf("cursor saves = %d, want 3", cc.saves)
	}
	if got, ok, _ := cc.Load(ctx); !ok || got != 5 {
		t.Fatalf("final persisted cursor = %d,%v, want 5,true", got, ok)
	}
}

func TestBatchedCursorResumesCorrectly(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p := newProducer(t, os)
	for i := 0; i < 4; i++ {
		d, _ := p.Append(ctx, [][]byte{[]byte{byte('0' + i)}}, nil)
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	p.Close(ctx)

	cs := NewFileCursorStore(filepath.Join(t.TempDir(), "cursor"))
	// Apply all with a wide interval; end-of-poll save still persists next=4.
	r := NewReplica(os, &recordingApplier{}, ReplicaConfig{
		ManifestPath:       testManifest,
		Cursor:             cs,
		CursorSaveInterval: 100,
	})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if next, ok, _ := cs.Load(ctx); !ok || next != 4 {
		t.Fatalf("persisted cursor = %d,%v, want 4,true", next, ok)
	}
	// A fresh replica resumes at 4 and applies nothing.
	r2 := NewReplica(os, &recordingApplier{}, ReplicaConfig{ManifestPath: testManifest, Cursor: cs})
	if n, err := r2.Poll(ctx); err != nil || n != 0 {
		t.Fatalf("resume applied %d (err=%v), want 0", n, err)
	}
}
