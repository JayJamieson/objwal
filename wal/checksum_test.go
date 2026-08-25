package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// TestSegmentChecksumDetects walks every byte position in a v2 segment,
// flipping one bit at a time, and requires that the corruption is either
// detected or provably harmless. A silent wrong-content decode is a failure.
func TestSegmentChecksumDetects(t *testing.T) {
	want := [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie-longer-record")}
	enc, err := encodeSegment(want, CompressionNone, false)
	if err != nil {
		t.Fatal(err)
	}
	if v := binary.LittleEndian.Uint16(enc[len(enc)-2:]); v != segmentFormatVersion {
		t.Fatalf("expected v%d segments by default, got v%d", segmentFormatVersion, v)
	}

	silent := 0
	for i := 0; i < len(enc); i++ {
		bad := append([]byte(nil), enc...)
		bad[i] ^= 0x01
		got, err := decodeSegment(bad)
		if err != nil {
			continue // detected
		}
		if !equalRecords(got, want) {
			t.Errorf("byte %d: SILENT wrong content: %q", i, got)
			silent++
		}
	}
	if silent > 0 {
		t.Fatalf("%d byte positions decoded to wrong content without an error", silent)
	}
}

// TestSegmentChecksumIsErrCorrupt pins the error type: a caller must be able to
// tell corruption from a transport failure, because retrying a GET on corrupt
// bytes just re-reads the same bytes.
func TestSegmentChecksumIsErrCorrupt(t *testing.T) {
	enc, err := encodeSegment([][]byte{[]byte("payload")}, CompressionNone, false)
	if err != nil {
		t.Fatal(err)
	}
	enc[2] ^= 0xFF
	_, err = decodeSegment(enc)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

// TestSegmentV1StillReadable keeps the buffer batch-v1 compatibility promise:
// v1 objects written before this change, or by an upstream buffer, must
// continue to decode.
func TestSegmentV1StillReadable(t *testing.T) {
	want := [][]byte{[]byte("one"), []byte("two")}
	for _, comp := range []Compression{CompressionNone, CompressionZstd} {
		legacy, err := encodeSegment(want, comp, true)
		if err != nil {
			t.Fatal(err)
		}
		if v := binary.LittleEndian.Uint16(legacy[len(legacy)-2:]); v != segmentLegacyVersion {
			t.Fatalf("legacy encode wrote v%d", v)
		}
		got, err := decodeSegment(legacy)
		if err != nil {
			t.Fatalf("comp %d: %v", comp, err)
		}
		if !equalRecords(got, want) {
			t.Fatalf("comp %d: round-trip mismatch: %q", comp, got)
		}
	}
}

func TestSegmentRoundTripBothCodecs(t *testing.T) {
	want := [][]byte{[]byte("a"), {}, []byte("ccc")}
	for _, comp := range []Compression{CompressionNone, CompressionZstd} {
		enc, err := encodeSegment(want, comp, false)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeSegment(enc)
		if err != nil {
			t.Fatalf("comp %d: %v", comp, err)
		}
		if !equalRecords(got, want) {
			t.Fatalf("comp %d: mismatch %q vs %q", comp, got, want)
		}
	}
}

// TestManifestChecksumDetects does the same bit-flip sweep over a v4 manifest.
// The manifest is the ordering authority, so a silent flip here misdirects the
// whole log rather than one batch of records.
func TestManifestChecksumDetects(t *testing.T) {
	m := NewManifest()
	m.SetEpoch(7)
	m.SetSnapshot(SnapshotPointer{Location: "snap/base", ThroughSeq: 2, CreatedUnixMs: 11})
	for i := 0; i < 4; i++ {
		if _, err := m.Append("wal/seg/abc", []RecordMeta{{StartIndex: 0, Payload: []byte("md")}}, 2); err != nil {
			t.Fatal(err)
		}
	}
	enc, err := m.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if v := binary.LittleEndian.Uint16(enc[len(enc)-2:]); v != checksumFooterVersion {
		t.Fatalf("expected v%d manifests, got v%d", checksumFooterVersion, v)
	}
	base, err := ParseManifest(enc)
	if err != nil {
		t.Fatal(err)
	}
	wantEntries, _ := base.Entries()

	silent := 0
	for i := 0; i < len(enc); i++ {
		bad := append([]byte(nil), enc...)
		bad[i] ^= 0x01
		p, err := ParseManifest(bad)
		if err != nil {
			continue
		}
		got, err := p.Entries()
		if err != nil {
			continue
		}
		if p.Epoch() != base.Epoch() || p.NextSequence() != base.NextSequence() ||
			p.Snapshot() != base.Snapshot() || len(got) != len(wantEntries) {
			t.Errorf("byte %d: SILENT manifest divergence", i)
			silent++
			continue
		}
		for j := range got {
			if got[j].Sequence != wantEntries[j].Sequence || got[j].Location != wantEntries[j].Location {
				t.Errorf("byte %d: SILENT entry divergence at %d", i, j)
				silent++
				break
			}
		}
	}
	if silent > 0 {
		t.Fatalf("%d byte positions parsed to a different manifest without an error", silent)
	}
}

// TestManifestLegacyVersionsStillParse: v1/v2/v3 objects carry no digest and
// must keep parsing, so an existing deployment upgrades in place. The first
// re-commit rewrites the object as v4.
func TestManifestLegacyVersionsStillParse(t *testing.T) {
	for _, version := range []uint16{bufferFooterVersion, snapshotFooterVersion, recordCountFooterVersion} {
		enc := buildLegacyManifest(t, version)
		m, err := ParseManifest(enc)
		if err != nil {
			t.Fatalf("v%d: %v", version, err)
		}
		if m.Epoch() != 5 {
			t.Fatalf("v%d: epoch %d, want 5", version, m.Epoch())
		}
		// A re-commit must upgrade the object to v4 with a valid digest.
		out, err := m.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if v := binary.LittleEndian.Uint16(out[len(out)-2:]); v != checksumFooterVersion {
			t.Fatalf("v%d: re-commit wrote v%d, want v%d", version, v, checksumFooterVersion)
		}
		if _, err := ParseManifest(out); err != nil {
			t.Fatalf("v%d: upgraded manifest does not parse: %v", version, err)
		}
	}
}

// buildLegacyManifest hand-rolls a manifest in a pre-v4 footer format. v1 and
// v2 encode entries without the per-entry Count field; v3 uses the current
// entry encoding. None of them carry a digest.
func buildLegacyManifest(t *testing.T, version uint16) []byte {
	t.Helper()
	var body []byte
	switch version {
	case bufferFooterVersion, snapshotFooterVersion:
		body = encodeLegacyEntryForTest(Entry{Sequence: 0, Location: "wal/seg/x"})
	case recordCountFooterVersion:
		var err error
		body, err = encodeEntry(nil, Entry{Sequence: 0, Count: 1, Location: "wal/seg/x"})
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported legacy version %d", version)
	}
	if version != bufferFooterVersion {
		loc := []byte("s")
		body = append(body, loc...)
		body = binary.LittleEndian.AppendUint16(body, uint16(len(loc)))
		body = binary.LittleEndian.AppendUint64(body, 1) // through_seq
		body = binary.LittleEndian.AppendUint64(body, 2) // created_ms
	}
	body = binary.LittleEndian.AppendUint32(body, 1) // entries_count
	body = binary.LittleEndian.AppendUint64(body, 1) // next_sequence
	body = binary.LittleEndian.AppendUint64(body, 5) // epoch
	return binary.LittleEndian.AppendUint16(body, version)
}

func TestCRCTableIsCastagnoli(t *testing.T) {
	if crc32.Checksum([]byte("123456789"), castagnoli) != 0xE3069283 {
		t.Fatal("castagnoli table is not CRC-32C")
	}
}

func equalRecords(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
