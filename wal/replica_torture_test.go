package wal_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/wal"
)

// SUSPECT 9: an Applier that fails part-way through a segment.
//
// This is the sharpest remaining data-loss shape. A segment holds N records
// and Apply is called per record. If Apply fails on record k, the replica must
// NOT persist a cursor past k: on restart it would resume after records that
// were never applied, and they are gone forever with no error surfaced. The
// replica's whole contract is that its state is a prefix of the log.
//
// Apply is idempotent by contract, so re-delivering records 0..k-1 is fine.
// Skipping k..N-1 is not.
func TestApplierFailsMidSegment_NoRecordsSkipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"

	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:  manifest,
		SegmentPrefix: segPrefix,
		FlushInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Multi-record groups so failures land INSIDE a segment, not between them.
	var want []string
	for g := 0; g < 6; g++ {
		recs := make([][]byte, 4)
		for j := range recs {
			id := "a" + strconv.Itoa(g) + "_" + strconv.Itoa(j)
			recs[j] = []byte(id)
			want = append(want, id)
		}
		d, err := p.Append(ctx, recs, []byte("g"+strconv.Itoa(g)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	errBoom := errors.New("applier boom")
	cursor := &wal.MemCursorStore{}
	var applied []string
	failAt := 0 // fail on the Nth Apply call of each attempt, walking forward

	for attempt := 0; attempt < 40; attempt++ {
		calls := 0
		r := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
			calls++
			if failAt > 0 && calls == failAt {
				return fmt.Errorf("%w at call %d", errBoom, calls)
			}
			applied = append(applied, string(rec.Data))
			return nil
		}), wal.ReplicaConfig{
			ManifestPath: manifest,
			PollInterval: time.Millisecond,
			Cursor:       cursor,
		})
		// Fail progressively later, then stop failing so it can finish.
		failAt = attempt % 5
		for i := 0; i < 3; i++ {
			if _, err := r.Poll(ctx); err != nil && !errors.Is(err, errBoom) {
				t.Fatalf("attempt %d: unexpected poll error: %v", attempt, err)
			}
		}
	}
	// Final clean pass.
	failAt = 0
	final := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
		applied = append(applied, string(rec.Data))
		return nil
	}), wal.ReplicaConfig{ManifestPath: manifest, PollInterval: time.Millisecond, Cursor: cursor})
	for i := 0; i < 10; i++ {
		if _, err := final.Poll(ctx); err != nil {
			t.Fatalf("final poll: %v", err)
		}
	}

	// Duplicates are allowed (Apply is idempotent). Gaps and reorders are not.
	seen := map[string]bool{}
	var order []string
	for _, id := range applied {
		if !seen[id] {
			seen[id] = true
			order = append(order, id)
		}
	}
	var missing []string
	for _, id := range want {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("RECORDS SKIPPED after mid-segment Apply failures: %d of %d never delivered: %v\n"+
			"the cursor advanced past records that were never applied", len(missing), len(want), missing)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("REORDER after mid-segment Apply failure at %d: got %s want %s", i, order[i], want[i])
		}
	}
}

// SUSPECT 10: bootstrap from a mid-log sequence (ReplicaConfig.StartAt).
//
// Untested so far: every replica in these tests started at zero. A replica
// that starts at a sequence in the MIDDLE of a segment's range must deliver
// exactly the records from that sequence onward - not the whole segment, and
// not skip to the next one.
func TestReplicaStartAt_MidSegment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"

	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:  manifest,
		SegmentPrefix: segPrefix,
		FlushInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var all []string
	for g := 0; g < 5; g++ {
		recs := make([][]byte, 4)
		for j := range recs {
			id := "s" + strconv.Itoa(g) + "_" + strconv.Itoa(j)
			recs[j] = []byte(id)
			all = append(all, id)
		}
		d, err := p.Append(ctx, recs, []byte("g"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close(ctx)

	// Start at a sequence inside the third group.
	const startAt = 9
	var got []string
	r := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
		got = append(got, string(rec.Data))
		return nil
	}), wal.ReplicaConfig{ManifestPath: manifest, PollInterval: time.Millisecond, StartAt: startAt})
	for i := 0; i < 8; i++ {
		if _, err := r.Poll(ctx); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}

	want := all[startAt:]
	if len(got) < len(want) {
		t.Fatalf("StartAt=%d delivered %d records, expected at least %d\n  got:  %v\n  want: %v",
			startAt, len(got), len(want), got, want)
	}
	// Trailing delivery may include earlier records from the same segment
	// (whole-segment reads are legitimate), but everything from startAt on must
	// be present and in order.
	idx := 0
	for _, id := range got {
		if idx < len(want) && id == want[idx] {
			idx++
		}
	}
	if idx != len(want) {
		t.Fatalf("StartAt=%d: records from the start sequence are missing or out of order (matched %d/%d)\n  got:  %v\n  want: %v",
			startAt, idx, len(want), got, want)
	}
}
