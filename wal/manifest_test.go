package wal

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/JayJamieson/objwal/objectstore"
)

func mustBytes(t *testing.T, m *Manifest) []byte {
	t.Helper()
	b, err := m.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	return b
}

func TestEmptyManifestRoundTrip(t *testing.T) {
	m := NewManifest()
	got, err := ParseManifest(mustBytes(t, m))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if got.Count() != 0 || got.NextSequence() != 0 || got.Epoch() != 0 {
		t.Fatalf("unexpected empty state: count=%d next=%d epoch=%d", got.Count(), got.NextSequence(), got.Epoch())
	}
	if !got.Snapshot().IsZero() {
		t.Fatalf("expected zero snapshot, got %+v", got.Snapshot())
	}
}

func TestSnapshotPointerRoundTrip(t *testing.T) {
	m := NewManifest()
	if _, err := m.Append("seg/0000.batch", []RecordMeta{{StartIndex: 0, IngestionTimeMs: 111, Payload: []byte("a")}}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append("seg/0001.batch", nil, 1); err != nil {
		t.Fatal(err)
	}
	m.SetEpoch(7)
	snap := SnapshotPointer{Location: "snapshots/base-0001.ckpt", ThroughSeq: 1, CreatedUnixMs: 1_700_000_000_000}
	m.SetSnapshot(snap)

	got, err := ParseManifest(mustBytes(t, m))
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if got.Snapshot() != snap {
		t.Fatalf("snapshot mismatch:\n got %+v\nwant %+v", got.Snapshot(), snap)
	}
	if got.Epoch() != 7 {
		t.Fatalf("epoch = %d, want 7", got.Epoch())
	}
	if got.NextSequence() != 2 || got.Count() != 2 {
		t.Fatalf("next=%d count=%d, want 2/2", got.NextSequence(), got.Count())
	}
	entries, err := got.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Sequence != 0 || entries[1].Sequence != 1 {
		t.Fatalf("bad entries: %+v", entries)
	}
	if entries[0].Location != "seg/0000.batch" || len(entries[0].Metadata) != 1 ||
		string(entries[0].Metadata[0].Payload) != "a" || entries[0].Metadata[0].IngestionTimeMs != 111 {
		t.Fatalf("entry 0 round-trip wrong: %+v", entries[0])
	}
}

// A v1 (buffer) manifest must parse as a zero-snapshot v2 manifest, so the
// replica can bootstrap from a plain buffer queue.
func TestReadsBufferV1Footer(t *testing.T) {
	// Hand-build a v1 manifest: one legacy entry (no Count field) + 22-byte v1 footer.
	body := encodeLegacyEntryForTest(Entry{Sequence: 0, Location: "x", Metadata: nil})
	buf := append([]byte{}, body...)
	buf = binary.LittleEndian.AppendUint32(buf, 1)                   // entries_count
	buf = binary.LittleEndian.AppendUint64(buf, 1)                   // next_sequence
	buf = binary.LittleEndian.AppendUint64(buf, 42)                  // epoch
	buf = binary.LittleEndian.AppendUint16(buf, bufferFooterVersion) // version = 1

	m, err := ParseManifest(buf)
	if err != nil {
		t.Fatalf("ParseManifest(v1): %v", err)
	}
	if !m.Snapshot().IsZero() {
		t.Fatalf("v1 manifest should have zero snapshot, got %+v", m.Snapshot())
	}
	if m.Epoch() != 42 || m.NextSequence() != 1 || m.Count() != 1 {
		t.Fatalf("v1 fields wrong: epoch=%d next=%d count=%d", m.Epoch(), m.NextSequence(), m.Count())
	}
	es, err := m.Entries()
	if err != nil || len(es) != 1 || es[0].Location != "x" {
		t.Fatalf("v1 entries wrong: %+v err=%v", es, err)
	}
}

// The core-footer fields must occupy identical trailing offsets whether or not
// a snapshot block is present. This is the invariant that lets a reader detect
// the version from the last two bytes before deciding how to parse.
func TestCoreFooterOffsetsStable(t *testing.T) {
	withSnap := NewManifest()
	withSnap.SetEpoch(9)
	withSnap.SetSnapshot(SnapshotPointer{Location: "s", ThroughSeq: 3, CreatedUnixMs: 5})
	noSnap := NewManifest()
	noSnap.SetEpoch(9)

	a := mustBytes(t, withSnap)
	b := mustBytes(t, noSnap)
	// epoch lives at [n-10 : n-2] in both.
	ea := binary.LittleEndian.Uint64(a[len(a)-10 : len(a)-2])
	eb := binary.LittleEndian.Uint64(b[len(b)-10 : len(b)-2])
	if ea != 9 || eb != 9 {
		t.Fatalf("epoch offset unstable: withSnap=%d noSnap=%d", ea, eb)
	}
	// version is the final two bytes in both.
	if binary.LittleEndian.Uint16(a[len(a)-2:]) != FooterVersion ||
		binary.LittleEndian.Uint16(b[len(b)-2:]) != FooterVersion {
		t.Fatal("version not in final two bytes")
	}
}

func TestAppendPreservesSnapshot(t *testing.T) {
	m := NewManifest()
	for i := 0; i < 3; i++ {
		if _, err := m.Append("seg", nil, 1); err != nil {
			t.Fatal(err)
		}
	}
	snap := SnapshotPointer{Location: "base.ckpt", ThroughSeq: 2, CreatedUnixMs: 99}
	m.SetSnapshot(snap)
	// Round-trip through bytes (as a real commit would), then append more.
	m, err := ParseManifest(mustBytes(t, m))
	if err != nil {
		t.Fatal(err)
	}
	seq, err := m.Append("seg/new", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("appended seq = %d, want 3", seq)
	}
	got, err := ParseManifest(mustBytes(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if got.Snapshot() != snap {
		t.Fatalf("snapshot not preserved across append: got %+v want %+v", got.Snapshot(), snap)
	}
	if got.Count() != 4 || got.NextSequence() != 4 {
		t.Fatalf("count=%d next=%d, want 4/4", got.Count(), got.NextSequence())
	}
}

func TestTruncateThrough(t *testing.T) {
	m := NewManifest()
	for i := 0; i < 5; i++ {
		if _, err := m.Append("seg", []RecordMeta{{StartIndex: 0, Payload: []byte{byte(i)}}}, 1); err != nil {
			t.Fatal(err)
		}
	}
	m.SetSnapshot(SnapshotPointer{Location: "base.ckpt", ThroughSeq: 2, CreatedUnixMs: 1})
	removed, err := m.TruncateThrough(2)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Fatalf("removed = %d, want 3", removed)
	}
	// Survivors are sequences 3 and 4; nextSequence and snapshot unchanged.
	got, err := ParseManifest(mustBytes(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if got.Count() != 2 || got.NextSequence() != 5 {
		t.Fatalf("after truncate count=%d next=%d, want 2/5", got.Count(), got.NextSequence())
	}
	es, err := got.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 2 || es[0].Sequence != 3 || es[1].Sequence != 4 {
		t.Fatalf("survivors wrong: %+v", es)
	}
	if got.Snapshot().ThroughSeq != 2 {
		t.Fatalf("snapshot lost across truncate: %+v", got.Snapshot())
	}
	// A subsequent append continues the sequence.
	seq, err := got.Append("seg/after", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Fatalf("post-truncate append seq = %d, want 5", seq)
	}
}

func TestEntriesAfter(t *testing.T) {
	m := NewManifest()
	for i := 0; i < 4; i++ {
		if _, err := m.Append("seg", nil, 1); err != nil {
			t.Fatal(err)
		}
	}
	es, err := m.EntriesAfter(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 2 || es[0].Sequence != 2 || es[1].Sequence != 3 {
		t.Fatalf("EntriesAfter(1) = %+v, want seqs 2,3", es)
	}
}

func TestStoreCASRoundTrip(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	s := NewStore(os, "wal/manifest")

	// Fresh load: no object yet.
	m, ver, ok, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ok || ver != nil {
		t.Fatal("expected missing manifest on fresh load")
	}
	if _, err := m.Append("seg/0", nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, m); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Reload, append, commit under the observed version.
	m, ver, ok, err = s.Load(ctx)
	if err != nil || !ok || ver == nil {
		t.Fatalf("reload failed: ok=%v ver=%v err=%v", ok, ver, err)
	}
	m.SetSnapshot(SnapshotPointer{Location: "base.ckpt", ThroughSeq: 0, CreatedUnixMs: 7})
	if _, err := m.Append("seg/1", nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, m, ver); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// A stale committer (reusing the old version) must lose the CAS race.
	if err := s.Commit(ctx, m, ver); err == nil {
		t.Fatal("expected stale Commit to fail precondition")
	}

	final, _, _, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if final.Count() != 2 || final.Snapshot().Location != "base.ckpt" {
		t.Fatalf("final state wrong: count=%d snap=%+v", final.Count(), final.Snapshot())
	}
}

// encodeLegacyEntryForTest writes an entry in the pre-v3 (buffer/v2) layout,
// without the Count field, to exercise the legacy-read/upgrade path.
func encodeLegacyEntryForTest(e Entry) []byte {
	body := make([]byte, 0, 32)
	body = binary.LittleEndian.AppendUint64(body, e.Sequence)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(e.Location)))
	body = append(body, e.Location...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(e.Metadata)))
	for _, md := range e.Metadata {
		body = binary.LittleEndian.AppendUint32(body, md.StartIndex)
		body = binary.LittleEndian.AppendUint64(body, uint64(md.IngestionTimeMs))
		body = binary.LittleEndian.AppendUint32(body, uint32(len(md.Payload)))
		body = append(body, md.Payload...)
	}
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	return append(out, body...)
}
