package wal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// gatedPutStore blocks unconditional Puts (segment uploads) until release is
// closed; PutOpts (manifest commits) pass through. Used to hold a flush
// in-flight so the in-flight budget stays occupied.
type gatedPutStore struct {
	objectstore.ObjectStore
	release chan struct{}
}

func (g *gatedPutStore) Put(ctx context.Context, path string, data []byte) error {
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.ObjectStore.Put(ctx, path, data)
}

func TestBackpressureBlocksThenReleases(t *testing.T) {
	ctx := context.Background()
	gate := &gatedPutStore{ObjectStore: objectstore.NewInMemory(), release: make(chan struct{})}
	p, err := NewProducer(ctx, gate, ProducerConfig{
		ManifestPath:     testManifest,
		SegmentPrefix:    testPrefix,
		FlushInterval:    time.Hour, // no auto-flush; rely on size trigger
		FlushBytes:       1,         // each Append triggers a flush
		MaxInFlightBytes: 4,         // room for exactly one 4-byte record
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx) // gate is closed in the body below; Close's flush passes through

	// First Append fits (nothing in flight) and triggers a flush that blocks
	// on the gated segment Put, so its budget stays held.
	d1, err := p.Append(ctx, [][]byte{[]byte("AAAA")}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Second Append must block: budget is full and the first hasn't committed.
	type res struct {
		d   *Durability
		err error
	}
	done := make(chan res, 1)
	go func() {
		d, err := p.Append(ctx, [][]byte{[]byte("BBBB")}, nil)
		done <- res{d, err}
	}()
	select {
	case <-done:
		t.Fatal("second Append should have blocked on the in-flight budget")
	case <-time.After(100 * time.Millisecond):
		// still blocked, as expected
	}

	// Release the gate: the first flush completes, commits, frees budget, and
	// the second Append proceeds (its own flush then also runs through the now-
	// open gate).
	close(gate.release)
	r := <-done
	if r.err != nil {
		t.Fatalf("second Append should succeed once unblocked: %v", r.err)
	}
	if _, err := d1.Wait(ctx); err != nil {
		t.Fatalf("d1 durability: %v", err)
	}
	if _, err := r.d.Wait(ctx); err != nil {
		t.Fatalf("d2 durability: %v", err)
	}

	// Both landed, in order.
	app := &recordingApplier{}
	rep := NewReplica(gate, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := rep.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := app.datas(); len(got) != 2 || got[0] != "AAAA" || got[1] != "BBBB" {
		t.Fatalf("applied %v, want [AAAA BBBB]", got)
	}
}

func TestBackpressureCancelledWhileBlocked(t *testing.T) {
	gate := &gatedPutStore{ObjectStore: objectstore.NewInMemory(), release: make(chan struct{})}
	p, err := NewProducer(context.Background(), gate, ProducerConfig{
		ManifestPath:     testManifest,
		SegmentPrefix:    testPrefix,
		FlushInterval:    time.Hour,
		FlushBytes:       1,
		MaxInFlightBytes: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(gate.release); p.Close(context.Background()) }()

	if _, err := p.Append(context.Background(), [][]byte{[]byte("AAAA")}, nil); err != nil {
		t.Fatal(err)
	}

	// A blocked Append whose ctx is cancelled returns ctx.Err() promptly.
	cctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := p.Append(cctx, [][]byte{[]byte("BBBB")}, nil)
		errc <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err != context.Canceled {
			t.Fatalf("blocked Append should return context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Append did not return")
	}
}

func TestBackpressureCountCap(t *testing.T) {
	gate := &gatedPutStore{ObjectStore: objectstore.NewInMemory(), release: make(chan struct{})}
	p, err := NewProducer(context.Background(), gate, ProducerConfig{
		ManifestPath:       testManifest,
		SegmentPrefix:      testPrefix,
		FlushInterval:      time.Hour,
		FlushBytes:         1,
		MaxInFlightBytes:   1 << 30, // bytes not the binding signal here
		MaxInFlightBatches: 1,       // only one un-committed Append allowed
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(gate.release); p.Close(context.Background()) }()

	if _, err := p.Append(context.Background(), [][]byte{[]byte("x")}, nil); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	go func() {
		_, _ = p.Append(context.Background(), [][]byte{[]byte("y")}, nil)
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("second Append should block on the batch-count cap")
	case <-time.After(80 * time.Millisecond):
	}
}

// countingStore counts segment Puts vs manifest commits (PutOpts).
type countingStore struct {
	objectstore.ObjectStore
	mu      sync.Mutex
	puts    int
	putOpts int
}

func (c *countingStore) Put(ctx context.Context, path string, data []byte) error {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	return c.ObjectStore.Put(ctx, path, data)
}

func (c *countingStore) PutOpts(ctx context.Context, path string, data []byte, o objectstore.PutOptions) error {
	c.mu.Lock()
	c.putOpts++
	c.mu.Unlock()
	return c.ObjectStore.PutOpts(ctx, path, data, o)
}

func (c *countingStore) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts, c.putOpts
}

func TestCoalesceManySegmentsOneCommit(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{ObjectStore: objectstore.NewInMemory()}
	// 4-byte segment cap => each 4-byte Append rotates into its own segment;
	// ManifestAppendBatchSize 0 => all coalesce into one CAS.
	p, err := NewProducer(ctx, cs, ProducerConfig{
		ManifestPath:    testManifest,
		SegmentPrefix:   testPrefix,
		FlushInterval:   time.Hour, // hold everything for one flush at Close
		SegmentMaxBytes: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, putOptsAfterClaim := cs.counts()

	var last *Durability
	for _, w := range []string{"aaaa", "bbbb", "cccc"} {
		d, err := p.Append(ctx, [][]byte{[]byte(w)}, []byte(w))
		if err != nil {
			t.Fatal(err)
		}
		last = d
	}
	if err := p.Close(ctx); err != nil { // single flush drains all three
		t.Fatal(err)
	}
	if _, err := last.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	puts, putOpts := cs.counts()
	if puts != 3 {
		t.Fatalf("segment Puts = %d, want 3 (one per rotated segment)", puts)
	}
	if commits := putOpts - putOptsAfterClaim; commits != 1 {
		t.Fatalf("manifest commits during flush = %d, want 1 (coalesced)", commits)
	}

	// Replica sees three records, sequences 0,1,2, each with its own group meta.
	app := &recordingApplier{}
	rep := NewReplica(cs, app, ReplicaConfig{ManifestPath: testManifest})
	if _, err := rep.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.applied) != 3 {
		t.Fatalf("applied %d records, want 3", len(app.applied))
	}
	for i, w := range []string{"aaaa", "bbbb", "cccc"} {
		if string(app.applied[i].Data) != w || string(app.applied[i].GroupMeta) != w {
			t.Fatalf("record %d = %q/%q, want %q", i, app.applied[i].Data, app.applied[i].GroupMeta, w)
		}
		if app.applied[i].Sequence != uint64(i) {
			t.Fatalf("record %d sequence = %d, want %d", i, app.applied[i].Sequence, i)
		}
	}
}

func TestCoalesceRespectsBatchSize(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{ObjectStore: objectstore.NewInMemory()}
	// 3 segments, coalesce at most 2 per CAS => 2 commits (2 + 1).
	p, err := NewProducer(ctx, cs, ProducerConfig{
		ManifestPath:            testManifest,
		SegmentPrefix:           testPrefix,
		FlushInterval:           time.Hour,
		SegmentMaxBytes:         4,
		ManifestAppendBatchSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, putOptsAfterClaim := cs.counts()

	var last *Durability
	for _, w := range []string{"aaaa", "bbbb", "cccc"} {
		d, _ := p.Append(ctx, [][]byte{[]byte(w)}, nil)
		last = d
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := last.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	puts, putOpts := cs.counts()
	if puts != 3 {
		t.Fatalf("segment Puts = %d, want 3", puts)
	}
	if commits := putOpts - putOptsAfterClaim; commits != 2 {
		t.Fatalf("manifest commits = %d, want 2 (groups of 2 and 1)", commits)
	}
}
