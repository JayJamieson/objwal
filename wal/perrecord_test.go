package wal

import (
	"context"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// buildManifest appends segments with the given record counts and returns it.
func buildManifest(t *testing.T, counts ...int) *Manifest {
	t.Helper()
	m := NewManifest()
	for i, c := range counts {
		if _, err := m.Append("seg/"+itoa(i), nil, c); err != nil {
			t.Fatalf("Append(count=%d): %v", c, err)
		}
	}
	return m
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestManifestRangesAndLocate(t *testing.T) {
	// counts 3,1,4 => ranges [0,3),[3,4),[4,8); nextSequence 8.
	m := buildManifest(t, 3, 1, 4)
	if m.NextSequence() != 8 {
		t.Fatalf("nextSequence = %d, want 8", m.NextSequence())
	}
	entries, _ := m.Entries()
	wantBase := []uint64{0, 3, 4}
	wantCount := []uint32{3, 1, 4}
	for i, e := range entries {
		if e.Sequence != wantBase[i] || e.Count != wantCount[i] {
			t.Fatalf("entry %d = seq %d count %d, want %d/%d", i, e.Sequence, e.Count, wantBase[i], wantCount[i])
		}
	}
	// Locate every record sequence to its segment + within-segment offset.
	cases := []struct {
		seq    uint64
		seg    string
		offset int
		found  bool
	}{
		{0, "seg/0", 0, true},
		{1, "seg/0", 1, true},
		{2, "seg/0", 2, true},
		{3, "seg/1", 0, true},
		{4, "seg/2", 0, true},
		{7, "seg/2", 3, true},
		{8, "", 0, false}, // == nextSequence: past the end
		{99, "", 0, false},
	}
	for _, c := range cases {
		e, off, ok := m.Locate(c.seq)
		if ok != c.found {
			t.Fatalf("Locate(%d) found=%v, want %v", c.seq, ok, c.found)
		}
		if ok && (e.Location != c.seg || off != c.offset) {
			t.Fatalf("Locate(%d) = %s+%d, want %s+%d", c.seq, e.Location, off, c.seg, c.offset)
		}
	}
}

func TestEntriesContainingStartsAtRightSegment(t *testing.T) {
	m := buildManifest(t, 3, 1, 4) // ranges [0,3),[3,4),[4,8)
	cases := []struct {
		seq       uint64
		firstSeg  string
		numReturn int
	}{
		{0, "seg/0", 3},
		{2, "seg/0", 3}, // seq 2 is in seg/0, so it and everything after
		{3, "seg/1", 2},
		{4, "seg/2", 1},
		{5, "seg/2", 1}, // mid seg/2
		{7, "seg/2", 1},
		{8, "", 0}, // past the end
	}
	for _, c := range cases {
		got, err := m.EntriesContaining(c.seq)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != c.numReturn {
			t.Fatalf("EntriesContaining(%d) returned %d entries, want %d", c.seq, len(got), c.numReturn)
		}
		if c.numReturn > 0 && got[0].Location != c.firstSeg {
			t.Fatalf("EntriesContaining(%d) first = %s, want %s", c.seq, got[0].Location, c.firstSeg)
		}
	}
}

// A consumer reading only the manifest can resume playback at an arbitrary
// record sequence, including mid-segment, and see exactly the records >= that
// sequence in order.
func TestReplicaResumesMidSegment(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p, err := NewProducer(ctx, os, ProducerConfig{
		ManifestPath:  testManifest,
		SegmentPrefix: testPrefix,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// One Append of 5 records => one segment, sequences 0..4.
	d, _ := p.Append(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}, nil)
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if s, err := d.Wait(ctx); err != nil || s != 0 {
		t.Fatalf("durability = %d,%v, want 0,nil", s, err)
	}

	// Resume from sequence 2 (mid-segment): expect c,d,e at sequences 2,3,4.
	app := &recordingApplier{}
	r := NewReplica(os, app, ReplicaConfig{ManifestPath: testManifest, StartAt: 2})
	n, err := r.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("applied %d, want 3", n)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	want := []struct {
		data string
		seq  uint64
	}{{"c", 2}, {"d", 3}, {"e", 4}}
	for i, w := range want {
		if string(app.applied[i].Data) != w.data || app.applied[i].Sequence != w.seq {
			t.Fatalf("record %d = %q@%d, want %q@%d", i, app.applied[i].Data, app.applied[i].Sequence, w.data, w.seq)
		}
	}
}

// A legacy (Count==0) entry occupies a single sequence slot and applies all its
// records at that one sequence - the pre-v3 whole-segment semantics - and a
// later native (per-record) entry continues contiguously after it.
func TestLegacyEntryWholeSegmentSemantics(t *testing.T) {
	m := NewManifest()
	// Inject a legacy entry by hand (Count 0) covering one slot at seq 0.
	enc, err := encodeEntry(nil, Entry{Sequence: 0, Count: 0, Location: "legacy", Metadata: nil})
	if err != nil {
		t.Fatal(err)
	}
	m.base = enc
	m.baseCount = 1
	m.nextSequence = 1 // legacy entry occupied [0,1)

	// Native append continues at seq 1 with 3 records => [1,4).
	if _, err := m.Append("native", nil, 3); err != nil {
		t.Fatal(err)
	}
	if m.NextSequence() != 4 {
		t.Fatalf("nextSequence = %d, want 4", m.NextSequence())
	}
	// Ranges must remain contiguous across the legacy/native boundary.
	entries, _ := m.Entries()
	if entries[0].End() != 1 || entries[1].Sequence != 1 || entries[1].End() != 4 {
		t.Fatalf("ranges not contiguous: %+v", entries)
	}
	// Locate within the legacy slot returns offset 0 (whole-segment).
	e, off, ok := m.Locate(0)
	if !ok || e.Location != "legacy" || off != 0 {
		t.Fatalf("Locate(0) = %s+%d,%v, want legacy+0,true", e.Location, off, ok)
	}
	// Locate into the native range addresses the right record.
	e, off, ok = m.Locate(3)
	if !ok || e.Location != "native" || off != 2 {
		t.Fatalf("Locate(3) = %s+%d,%v, want native+2,true", e.Location, off, ok)
	}
}

// A batched Append returns a single base sequence via Wait; WaitRange exposes
// the whole [First, First+Count) range so callers don't recompute offsets.
func TestDurabilityWaitRange(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	p, err := NewProducer(ctx, os, ProducerConfig{
		ManifestPath:  testManifest,
		SegmentPrefix: testPrefix,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	dA, _ := p.Append(ctx, [][]byte{[]byte("a0"), []byte("a1"), []byte("a2")}, nil) // 3 records
	dB, _ := p.Append(ctx, [][]byte{[]byte("b0")}, nil)                             // 1 record, coalesced after A
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if dA.Count() != 3 || dB.Count() != 1 {
		t.Fatalf("Count: A=%d B=%d, want 3,1", dA.Count(), dB.Count())
	}
	rA, err := dA.WaitRange(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rA.First != 0 || rA.Count != 3 || rA.End() != 3 || rA.Last() != 2 {
		t.Fatalf("A range = %+v end=%d last=%d, want First0 Count3 End3 Last2", rA, rA.End(), rA.Last())
	}
	if rA.At(0) != 0 || rA.At(2) != 2 || !rA.Contains(1) || rA.Contains(3) {
		t.Fatalf("A helpers: At0=%d At2=%d contains1=%v contains3=%v", rA.At(0), rA.At(2), rA.Contains(1), rA.Contains(3))
	}
	if got := rA.All(); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("A.All() = %v, want [0 1 2]", got)
	}
	// B coalesced behind A's 3 records, so it starts at sequence 3.
	rB, err := dB.WaitRange(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rB.First != 3 || rB.Count != 1 || rB.Last() != 3 {
		t.Fatalf("B range = %+v last=%d, want First3 Count1 Last3", rB, rB.Last())
	}
}
