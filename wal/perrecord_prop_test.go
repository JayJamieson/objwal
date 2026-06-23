package wal

import (
	"context"
	"testing"
	"testing/quick"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// clampCounts maps arbitrary generated values to valid record counts (1..8).
func clampCounts(raw []uint16) []int {
	out := make([]int, len(raw))
	for i, v := range raw {
		out[i] = int(v%8) + 1
	}
	return out
}

func manifestFromCounts(counts []int) (*Manifest, uint64) {
	m := NewManifest()
	var sum uint64
	for i, c := range counts {
		_, _ = m.Append("s"+itoa(i), nil, c)
		sum += uint64(c)
	}
	return m, sum
}

// INVARIANT: entries tile the record-sequence space [0, NextSequence)
// contiguously - no gaps, no overlaps - and NextSequence == sum of counts.
func TestProp_RangesPartitionContiguously(t *testing.T) {
	f := func(raw []uint16) bool {
		counts := clampCounts(raw)
		m, sum := manifestFromCounts(counts)
		if m.NextSequence() != sum {
			return false
		}
		entries, err := m.Entries()
		if err != nil || len(entries) != len(counts) {
			return false
		}
		var expect uint64
		for i, e := range entries {
			if e.Sequence != expect || e.Count != uint32(counts[i]) {
				return false
			}
			expect = e.End()
		}
		return expect == sum
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 400}); err != nil {
		t.Error(err)
	}
}

// INVARIANT: Locate is total over [0, NextSequence) - every record sequence
// resolves to the unique entry containing it with the correct within-segment
// offset - and returns not-found at or beyond NextSequence.
func TestProp_LocateIsTotalAndExact(t *testing.T) {
	f := func(raw []uint16, beyond uint16) bool {
		counts := clampCounts(raw)
		m, sum := manifestFromCounts(counts)
		entries, _ := m.Entries()
		for _, e := range entries {
			for s := e.Sequence; s < e.End(); s++ {
				le, off, ok := m.Locate(s)
				if !ok || le.Location != e.Location || uint64(off) != s-e.Sequence {
					return false
				}
			}
		}
		if _, _, ok := m.Locate(sum); ok { // == NextSequence is past the end
			return false
		}
		if _, _, ok := m.Locate(sum + uint64(beyond) + 1); ok {
			return false
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Error(err)
	}
}

// INVARIANT: EntriesContaining(seq) returns exactly the contiguous suffix of
// entries whose range end exceeds seq, and (for in-range seq) the first one
// contains seq.
func TestProp_EntriesContainingIsCorrectSuffix(t *testing.T) {
	f := func(raw []uint16, probe uint16) bool {
		counts := clampCounts(raw)
		m, sum := manifestFromCounts(counts)
		if sum == 0 {
			got, _ := m.EntriesContaining(0)
			return len(got) == 0
		}
		seq := uint64(probe) % (sum + 1) // 0..sum
		got, err := m.EntriesContaining(seq)
		if err != nil {
			return false
		}
		all, _ := m.Entries()
		// Expected: entries with End() > seq, preserving order.
		var want []Entry
		for _, e := range all {
			if e.End() > seq {
				want = append(want, e)
			}
		}
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i].Sequence != want[i].Sequence {
				return false
			}
		}
		if seq < sum && (len(got) == 0 || !got[0].Contains(seq)) {
			return false
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Error(err)
	}
}

// INVARIANT: Bytes -> ParseManifest is a faithful round trip for entries
// (sequence, count, location, metadata), nextSequence, and epoch.
func TestProp_RoundTripPreservesEverything(t *testing.T) {
	f := func(raw []uint16, epoch uint64) bool {
		counts := clampCounts(raw)
		m := NewManifest()
		m.SetEpoch(epoch)
		for i, c := range counts {
			md := []RecordMeta{{StartIndex: uint32(i), IngestionTimeMs: int64(i * 7), Payload: []byte("p" + itoa(i))}}
			if _, err := m.Append("loc"+itoa(i), md, c); err != nil {
				return false
			}
		}
		b, err := m.Bytes()
		if err != nil {
			return false
		}
		m2, err := ParseManifest(b)
		if err != nil {
			return false
		}
		if m2.NextSequence() != m.NextSequence() || m2.Epoch() != m.Epoch() || m2.Count() != m.Count() {
			return false
		}
		e1, _ := m.Entries()
		e2, _ := m2.Entries()
		if len(e1) != len(e2) {
			return false
		}
		for i := range e1 {
			if e1[i].Sequence != e2[i].Sequence || e1[i].Count != e2[i].Count || e1[i].Location != e2[i].Location {
				return false
			}
			if len(e1[i].Metadata) != len(e2[i].Metadata) {
				return false
			}
			for j := range e1[i].Metadata {
				a, b := e1[i].Metadata[j], e2[i].Metadata[j]
				if a.StartIndex != b.StartIndex || a.IngestionTimeMs != b.IngestionTimeMs || string(a.Payload) != string(b.Payload) {
					return false
				}
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Error(err)
	}
}

// INVARIANT: TruncateThrough removes exactly the fully-superseded entries, keeps
// every entry with a live record, leaves survivors contiguous, preserves
// NextSequence, and keeps Locate correct for surviving sequences.
func TestProp_TruncatePreservesContiguityAndLiveness(t *testing.T) {
	f := func(raw []uint16, throughRaw uint32) bool {
		counts := clampCounts(raw)
		if len(counts) == 0 {
			return true
		}
		m, sum := manifestFromCounts(counts)
		through := uint64(throughRaw) % (sum + 2) // 0..sum+1
		before, _ := m.Entries()
		removed, err := m.TruncateThrough(through)
		if err != nil {
			return false
		}
		after, _ := m.Entries()
		if len(before)-removed != len(after) {
			return false
		}
		if m.NextSequence() != sum {
			return false
		}
		for i, e := range after {
			if e.End()-1 <= through { // a kept entry must have a record > through
				return false
			}
			if i > 0 && e.Sequence != after[i-1].End() { // survivors stay contiguous
				return false
			}
			for s := e.Sequence; s < e.End(); s++ {
				le, _, ok := m.Locate(s)
				if !ok || le.Location != e.Location {
					return false
				}
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Error(err)
	}
}

// INVARIANT (end to end): replaying from ANY record sequence X yields exactly
// the records with sequence >= X, in order, with contiguous sequences X, X+1,
// ... - across segment rotation and resuming mid-segment. This ties producer
// (counts, coalescing, rotation) to manifest (ranges) to replica (per-record
// sequencing, mid-segment skip).
func TestProp_ReplayFromAnyPointMatches(t *testing.T) {
	f := func(raw []uint8, startRaw uint16) bool {
		ctx := context.Background()
		os := objectstore.NewInMemory()
		p, err := NewProducer(ctx, os, ProducerConfig{
			ManifestPath:    testManifest,
			SegmentPrefix:   testPrefix,
			FlushInterval:   time.Hour,
			SegmentMaxBytes: 8, // force rotation into multiple segments
		})
		if err != nil {
			return false
		}
		var all [][]byte
		for i, r := range raw {
			n := int(r%4) + 1
			recs := make([][]byte, n)
			for j := 0; j < n; j++ {
				rec := []byte(itoa(i) + "_" + itoa(j))
				recs[j] = rec
				all = append(all, rec)
			}
			if _, err := p.Append(ctx, recs, nil); err != nil {
				return false
			}
		}
		if err := p.Close(ctx); err != nil {
			return false
		}
		total := uint64(len(all))
		if total == 0 {
			return true
		}
		start := uint64(startRaw) % total

		app := &recordingApplier{}
		rr := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest, StartAt: start})
		if _, err := rr.Poll(ctx); err != nil {
			return false
		}
		app.mu.Lock()
		defer app.mu.Unlock()
		if uint64(len(app.applied)) != total-start {
			return false
		}
		for k, rec := range app.applied {
			wantSeq := start + uint64(k)
			if rec.Sequence != wantSeq || string(rec.Data) != string(all[wantSeq]) {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 60}); err != nil {
		t.Error(err)
	}
}
