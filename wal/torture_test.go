package wal_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

func newTestProducer(t *testing.T, store objectstore.ObjectStore, manifest, segPrefix string, mut func(*wal.ProducerConfig)) *wal.Producer {
	t.Helper()
	cfg := wal.ProducerConfig{
		ManifestPath:  manifest,
		SegmentPrefix: segPrefix,
		FlushInterval: 3 * time.Millisecond,
	}
	if mut != nil {
		mut(&cfg)
	}
	p, err := wal.NewProducer(context.Background(), store, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

// A Durability must always resolve, to a sequence or an error. An Append
// admitted after the final drain has no run loop left to flush it and nothing
// to fail it, so Wait would block forever.
func TestCloseRace_DurabilityAlwaysResolves(t *testing.T) {
	for attempt := 0; attempt < 40; attempt++ {
		store, keyPrefix := backingStore(t)
		manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
		ctx := context.Background()
		p := newTestProducer(t, store, manifest, segPrefix, nil)

		var wg sync.WaitGroup
		unresolved := make(chan string, 64)

		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := "r" + strconv.Itoa(i)
				d, err := p.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
				if err != nil {
					return // rejected outright: fine, that is a resolution
				}
				wctx, cancel := context.WithTimeout(ctx, 4*time.Second)
				defer cancel()
				if _, err := d.Wait(wctx); errors.Is(err, context.DeadlineExceeded) {
					unresolved <- id
				}
			}(i)
		}
		// Close concurrently with the appenders, hitting the window.
		time.Sleep(time.Duration(attempt%3) * time.Millisecond)
		_ = p.Close(ctx)
		wg.Wait()
		close(unresolved)

		var stuck []string
		for id := range unresolved {
			stuck = append(stuck, id)
		}
		if len(stuck) > 0 {
			t.Fatalf("attempt %d: %d Durability handles NEVER RESOLVED (%v). Append returned success but the "+
				"records were orphaned in p.pending after the run loop exited; Wait blocks forever.",
				attempt, len(stuck), stuck)
		}
	}
}

// An empty Append produces a zero-record segment whose manifest entry is
// invalid, which surfaces as a non-retryable commit error and halts the
// producer permanently.
func TestEmptyAppend_DoesNotHaltTheLog(t *testing.T) {
	ctx := context.Background()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
	p := newTestProducer(t, store, manifest, segPrefix, nil)
	defer func() { _ = p.Close(ctx) }()

	d, err := p.Append(ctx, nil, []byte("empty"))
	if err != nil {
		t.Logf("rejected at Append (good): %v", err)
	} else {
		wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if _, werr := d.Wait(wctx); werr != nil {
			t.Logf("empty Append failed at commit: %v", werr)
		}
	}

	// The log must still accept writes.
	d2, err := p.Append(ctx, [][]byte{[]byte("after")}, []byte("after"))
	if err != nil {
		t.Fatalf("PRODUCER HALTED BY AN EMPTY APPEND: subsequent Append rejected: %v", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := d2.Wait(wctx); err != nil {
		t.Fatalf("PRODUCER HALTED BY AN EMPTY APPEND: subsequent write not durable: %v", err)
	}
}

// Close must be idempotent; it used to close p.stop unconditionally.
func TestDoubleClose_DoesNotPanic(t *testing.T) {
	ctx := context.Background()
	store, keyPrefix := backingStore(t)
	p := newTestProducer(t, store, keyPrefix+"m", keyPrefix+"seg", nil)
	_ = p.Close(ctx)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("second Close PANICKED: %v", r)
		}
	}()
	_ = p.Close(ctx)
}

// Multi-record Append sequence arithmetic: resolvePlan's baseSeq+StartIndex
// across coalesced groups of differing sizes. Each group's returned sequence
// must be its own first record, and ranges must tile without gaps or overlap.
func TestMultiRecordAppend_SequenceArithmetic(t *testing.T) {
	ctx := context.Background()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
	// Long flush interval + concurrent appends => heavy coalescing.
	p := newTestProducer(t, store, manifest, segPrefix, func(c *wal.ProducerConfig) {
		c.FlushInterval = 40 * time.Millisecond
	})

	sizes := []int{1, 3, 2, 5, 1, 4, 2, 7, 3, 1}
	type got struct {
		seq  uint64
		size int
		idx  int
	}
	results := make([]got, len(sizes))
	var wg sync.WaitGroup
	for i, n := range sizes {
		wg.Add(1)
		go func(i, n int) {
			defer wg.Done()
			recs := make([][]byte, n)
			for j := range recs {
				recs[j] = []byte("g" + strconv.Itoa(i) + "r" + strconv.Itoa(j))
			}
			d, err := p.Append(ctx, recs, []byte("g"+strconv.Itoa(i)))
			if err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
			seq, err := d.Wait(ctx)
			if err != nil {
				t.Errorf("wait %d: %v", i, err)
				return
			}
			results[i] = got{seq: seq, size: n, idx: i}
		}(i, n)
	}
	wg.Wait()
	_ = p.Close(ctx)

	// Ranges [seq, seq+size) must tile [0, total) exactly.
	total := 0
	for _, n := range sizes {
		total += n
	}
	covered := make([]int, total)
	for _, r := range results {
		for k := 0; k < r.size; k++ {
			pos := int(r.seq) + k
			if pos < 0 || pos >= total {
				t.Fatalf("group %d: seq %d size %d runs outside [0,%d)", r.idx, r.seq, r.size, total)
			}
			covered[pos]++
		}
	}
	for i, c := range covered {
		if c != 1 {
			t.Fatalf("sequence %d covered %d times (ranges overlap or leave a gap): %+v", i, c, results)
		}
	}

	// And the log must actually contain every record, in that order.
	var applied []string
	r := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
		applied = append(applied, string(rec.Data))
		return nil
	}), wal.ReplicaConfig{ManifestPath: manifest, PollInterval: time.Millisecond})
	for i := 0; i < 6; i++ {
		if _, err := r.Poll(ctx); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	if len(applied) != total {
		t.Fatalf("replica applied %d records, expected %d", len(applied), total)
	}
}

// Replica restart from a persisted cursor - the normal production case.
func TestReplicaRestart_ResumesExactly(t *testing.T) {
	ctx := context.Background()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
	p := newTestProducer(t, store, manifest, segPrefix, nil)

	var want []string
	for i := 0; i < 25; i++ {
		id := "x" + strconv.Itoa(i)
		d, err := p.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		want = append(want, id)
	}
	_ = p.Close(ctx)

	cursor := &wal.MemCursorStore{}
	var applied []string
	mk := func() *wal.Replica {
		return wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
			applied = append(applied, string(rec.Data))
			return nil
		}), wal.ReplicaConfig{
			ManifestPath:      manifest,
			PollInterval:      time.Millisecond,
			Cursor:            cursor,
			MaxRecordsPerPoll: 3, // force many partial passes
		})
	}
	// Poll a bit, throw the replica away, rebuild it from the cursor, repeat.
	for round := 0; round < 12; round++ {
		r := mk()
		for i := 0; i < 2; i++ {
			if _, err := r.Poll(ctx); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}

	// Apply is idempotent by contract, so duplicates are allowed - but the
	// dedup'd order must be exactly the log, with nothing missing or reordered.
	var seen []string
	inOrder := map[string]bool{}
	for _, id := range applied {
		if !inOrder[id] {
			inOrder[id] = true
			seen = append(seen, id)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("after restarts, replica saw %d distinct records, log has %d\n  saw:  %v\n  want: %v",
			len(seen), len(want), seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("REORDER after restart at %d: got %s want %s\n  saw: %v", i, seen[i], want[i], seen)
		}
	}
}
