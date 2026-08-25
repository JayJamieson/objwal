package wal_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// TestUploadFaults injects faults on segment PUTs rather than the manifest CAS.
// flush() commits only the contiguous successful prefix and halts at the first
// failed upload, since committing past a gap would reorder the log.
//
// Beyond linearizability:
//
//	INTEGRITY  every committed location exists in the store and is non-empty
//	DENSE      committed sequences are gapless and ascending
//	ACKED      a halt may fail pending writes, never an acked one
func TestUploadFaults(t *testing.T) {
	for _, seed := range seeds(t) {
		runUploadFaults(t, seed, 0.70, 0.0)
	}
}

// TestUploadFaults_Ambiguous: uploads that land and then report failure. The
// producer halts and orphans a complete object - wasteful, but must be correct.
func TestUploadFaults_Ambiguous(t *testing.T) {
	for _, seed := range seeds(t) {
		runUploadFaults(t, seed, 0.0, 0.70)
	}
}

func runUploadFaults(t *testing.T, seed uint64, clean, ambiguous float64) {
	t.Helper()
	const (
		clients   = 3
		perClient = 20
	)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	inner, keyPrefix := backingStore(t)
	manifest := keyPrefix + "wal/manifest"
	segPrefix := keyPrefix + "wal/seg"

	// Faults on the SEGMENT prefix only; the manifest CAS is left healthy so
	// any failure here is attributable to the upload path.
	sim := objectstore.NewSimStore(inner, seed, objectstore.Faults{
		FailClean:     clean,
		FailAmbiguous: ambiguous,
		KeySubstring:  "wal/seg",
	})

	newProducer := func() (*wal.Producer, error) {
		return wal.NewProducer(ctx, sim, wal.ProducerConfig{
			ManifestPath:            manifest,
			SegmentPrefix:           segPrefix,
			FlushInterval:           2 * time.Millisecond,
			SegmentMaxBytes:         1, // one Append group per segment: maximum rotation
			ManifestAppendBatchSize: 1,
			UploadConcurrency:       4,
			UploadMaxAttempts:       2,
			UploadInitialBackoff:    200 * time.Microsecond,
			ManifestInitialBackoff:  200 * time.Microsecond,
		})
	}

	rec := &recorder{}
	acked := map[string]uint64{}
	var ackedMu = make(chan struct{}, 1)
	ackedMu <- struct{}{}

	// A halt is expected here, so appenders fail over to a fresh producer.
	prod, err := newProducer()
	if err != nil {
		t.Fatalf("seed %d: producer: %v", seed, err)
	}
	producers := []*wal.Producer{prod}

	for c := 0; c < clients; c++ {
		for i := 0; i < perClient; i++ {
			id := "u" + strconv.Itoa(c) + "_" + strconv.Itoa(i)
			in := logInput{op: opAppend, id: id}
			call := now()
			d, aerr := prod.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
			var seq uint64
			var werr error
			if aerr == nil {
				seq, werr = d.Wait(ctx)
			}
			if aerr != nil || werr != nil {
				rec.add(c, in, call, logOutput{seq: seqUnknown}, now())
				// Halted: failover, exactly as a real deployment would.
				np, nerr := newProducer()
				if nerr != nil {
					t.Fatalf("seed %d: failover: %v", seed, nerr)
				}
				prod = np
				producers = append(producers, np)
				continue
			}
			rec.add(c, in, call, logOutput{seq: seq}, now())
			<-ackedMu
			acked[id] = seq
			ackedMu <- struct{}{}
		}
	}
	for _, p := range producers {
		_ = p.Close(ctx)
	}

	// Read the committed log through the raw store.
	m, _, ok, err := wal.NewStore(inner, manifest).Load(ctx)
	if err != nil || !ok {
		t.Fatalf("seed %d: load manifest: %v", seed, err)
	}
	entries, err := m.Entries()
	if err != nil {
		t.Fatalf("seed %d: entries: %v", seed, err)
	}

	// INTEGRITY + DENSE
	var committed string
	var wantSeq uint64
	for _, e := range entries {
		res, gerr := inner.Get(ctx, e.Location)
		if gerr != nil {
			t.Fatalf("seed %d: INTEGRITY VIOLATION: manifest commits %s at seq %d but the object is not in the store: %v",
				seed, e.Location, e.Sequence, gerr)
		}
		if len(res.Data) == 0 {
			t.Fatalf("seed %d: INTEGRITY VIOLATION: committed segment %s is empty", seed, e.Location)
		}
		if e.Sequence != wantSeq {
			t.Fatalf("seed %d: GAP: entry at seq %d, expected %d (halt-on-first-failure should make this impossible)",
				seed, e.Sequence, wantSeq)
		}
		wantSeq = e.End()
		for _, md := range e.Metadata {
			committed = push(committed, string(md.Payload))
		}
	}

	// ACKED: a halt may fail pending writes, never an acked one.
	for id, seq := range acked {
		if !has(committed, id) {
			t.Fatalf("seed %d: LOST ACKED WRITE: %s was acked at seq %d but is not in the committed log", seed, id, seq)
		}
	}

	// Orphans: segments in the store that no manifest entry references.
	objs, lerr := inner.List(ctx, keyPrefix+"wal/seg")
	orphans := 0
	if lerr == nil {
		live := map[string]bool{}
		for _, e := range entries {
			live[e.Location] = true
		}
		for _, o := range objs {
			if !live[o.Location] {
				orphans++
			}
		}
	}

	call := now()
	rec.add(0, logInput{op: opRead}, call, logOutput{log: committed}, now())

	if !porcupine.CheckOperations(logModel.ToModel(), rec.ops) {
		t.Fatalf("seed %d: NOT LINEARIZABLE under upload faults\n  log: %s", seed, committed)
	}
	st := sim.Stats()
	// Refuse to pass vacuously: if no upload ever exhausted its retries, the
	// halt-and-commit-only-the-prefix path under test never ran.
	if len(producers) == 1 {
		t.Fatalf("seed %d: no producer halted - the upload failure path was never exercised (%d clean/%d ambiguous faults injected, but retries absorbed them all)",
			seed, st.CleanFaults, st.AmbiguousFaults)
	}
	t.Logf("seed %d OK: %d acked, log=%d, %d producers (%d halts), %d clean/%d ambiguous upload faults, %d orphaned segments",
		seed, len(acked), size(committed), len(producers), len(producers)-1,
		st.CleanFaults, st.AmbiguousFaults, orphans)
	if orphans > 0 && !strings.Contains(t.Name(), "nothing") {
		t.Logf("  note: %d uploaded-but-uncommitted segments leaked; nothing in wal/ ever calls store.Delete", orphans)
	}
}
