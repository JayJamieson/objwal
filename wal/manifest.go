// Package wal turns the buffer's queue primitives into a replication log.
//
// The buffer is a destructive single-consumer queue; replication needs a
// non-destructively *tailed* log with a base snapshot for bootstrap. This
// package reuses the buffer's object-store abstraction and append-friendly
// manifest, and extends the manifest with a snapshot pointer (footer v2) and
// per-record sequencing (footer v3) so a single CAS-protected object describes
// both the live WAL tail and the base a new or lagging replica restores from,
// and lets a consumer address playback to an individual record sequence.
//
// Sequence model (footer v3): each entry owns a contiguous half-open range of
// record sequences [Sequence, Sequence+Count). A segment with N framed records
// advances the log's nextSequence by N, and record i in that segment has
// sequence Sequence+i. Entries tile the sequence space with no gaps or overlaps,
// so a consumer can read only the manifest, find the entry whose range contains
// a target sequence (Locate), and start reading segments from there.
//
// Legacy footers (v1 buffer, v2) used one sequence per entry; their entries are
// read back with Count==0, meaning "this segment occupies a single sequence
// slot and all its records share that sequence" - the original whole-segment
// semantics, preserved so old manifests still replay correctly.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

// Footer/format constants.
const (
	// FooterVersion is the version this package writes: v3 plus a crc32c over
	// the whole object. The manifest is the ordering authority, so a silent
	// flip here misdirects the whole log, not one batch of records.
	FooterVersion uint16 = 4
	// checksumFooterVersion (v4) adds the digest; recordCountFooterVersion
	// (v3) carries the snapshot block and per-entry Count;
	// snapshotFooterVersion (v2) carries the snapshot block with one sequence
	// per entry; bufferFooterVersion (v1) is the buffer's footer with neither.
	checksumFooterVersion    uint16 = 4
	recordCountFooterVersion uint16 = 3
	snapshotFooterVersion    uint16 = 2
	bufferFooterVersion      uint16 = 1

	entriesCountSize = 4
	sequenceSize     = 8
	epochSize        = 8
	versionSize      = 2
	checksumSize     = 4
	// coreFooterSize is the trailing block shared by v1-v3. v4 inserts a
	// crc32c between epoch and version, so use coreFooter(version) when
	// computing offsets from the end.
	coreFooterSize   = entriesCountSize + sequenceSize + epochSize + versionSize                // 22
	coreFooterSizeV4 = entriesCountSize + sequenceSize + epochSize + checksumSize + versionSize // 26

	snapLocLenSize  = 2
	snapThroughSize = 8
	snapCreatedSize = 8
	snapFixedSize   = snapLocLenSize + snapThroughSize + snapCreatedSize // 18

	locationLenSize   = 2
	countFieldSize    = 4 // per-entry record Count (v3 entries only)
	metadataCountSize = 4
	startIndexSize    = 4
	ingestionTimeSize = 8
	metadataLenSize   = 4
	entryBodyLenSize  = 4
)

// SnapshotPointer identifies the base snapshot a replica bootstraps from.
// The zero value (empty Location) means "no snapshot has been taken yet".
type SnapshotPointer struct {
	Location      string
	ThroughSeq    uint64
	CreatedUnixMs int64
}

// IsZero reports whether no snapshot has been recorded.
func (s SnapshotPointer) IsZero() bool { return s.Location == "" }

// RecordMeta is a per-range annotation attached to a manifest entry,
// delimiting the run of framed records beginning at StartIndex.
type RecordMeta struct {
	StartIndex      uint32
	IngestionTimeMs int64
	Payload         []byte
}

// Entry is one committed manifest entry: a segment object plus the metadata
// ranges describing the framed records it holds. The entry owns the record
// sequence range [Sequence, Sequence+RangeSize()); record i has sequence
// Sequence+i. Count is the number of framed records in the segment; Count==0
// is the legacy per-entry sentinel (one sequence slot, all records share
// Sequence).
type Entry struct {
	Sequence uint64
	Count    uint32
	Location string
	Metadata []RecordMeta
}

// RangeSize is the number of record sequences the entry occupies: Count for a
// per-record (v3) entry, or 1 for a legacy (Count==0) entry.
func (e Entry) RangeSize() uint64 {
	if e.Count == 0 {
		return 1
	}
	return uint64(e.Count)
}

// End is the exclusive upper bound of the entry's record-sequence range.
func (e Entry) End() uint64 { return e.Sequence + e.RangeSize() }

// Contains reports whether record sequence seq falls within this entry.
func (e Entry) Contains(seq uint64) bool { return seq >= e.Sequence && seq < e.End() }

// Manifest is the in-memory, mutable view of a replication-log manifest. It
// preserves the buffer's O(1) append shape: existing entries are held as raw
// bytes (base) and new entries accumulate in a side buffer (appended); only a
// snapshot change or truncation rewrites base. base/appended are always held in
// the current (v3) entry encoding; a parsed legacy manifest is normalized on
// load.
//
// Manifest is not safe for concurrent mutation; the writer owns one instance.
type Manifest struct {
	base          []byte
	baseCount     int
	appended      []byte
	appendedCount int
	nextSequence  uint64
	epoch         uint64
	snapshot      SnapshotPointer
}

// NewManifest returns an empty manifest at epoch 0 with no snapshot.
func NewManifest() *Manifest { return &Manifest{} }

// ParseManifest decodes a serialized manifest. It accepts v4 (this package),
// v3, v2 and v1. Legacy (v1/v2) entries are normalized in memory to the v3
// encoding with Count==0, so downstream decoding is uniform; a re-commit
// upgrades the object to v4.
// coreFooter returns the size of the trailing core block for a footer
// version. v4 carries a crc32c that the earlier versions do not.
func coreFooter(version uint16) int {
	if version == checksumFooterVersion {
		return coreFooterSizeV4
	}
	return coreFooterSize
}

func ParseManifest(data []byte) (*Manifest, error) {
	if len(data) < coreFooterSize {
		return nil, fmt.Errorf("wal: manifest too short for footer (%d bytes)", len(data))
	}
	n := len(data)
	version := binary.LittleEndian.Uint16(data[n-versionSize:])
	core := coreFooter(version)
	if n < core {
		return nil, fmt.Errorf("wal: manifest too short for v%d footer (%d bytes)", version, n)
	}
	// Verify first: every field below, including the entry count that drives
	// the decode loop, is inside the digest.
	if version == checksumFooterVersion {
		want := binary.LittleEndian.Uint32(data[n-versionSize-checksumSize : n-versionSize])
		covered := data[:n-versionSize-checksumSize]
		if got := crc32.Checksum(covered, castagnoli); got != want {
			return nil, fmt.Errorf("%w: manifest crc32c %08x, stored %08x", ErrCorrupt, got, want)
		}
	}
	// v4 shifts the core fields down by the checksum before the version word.
	tail := n - versionSize
	if version == checksumFooterVersion {
		tail -= checksumSize
	}
	epoch := binary.LittleEndian.Uint64(data[tail-epochSize : tail])
	nextSeq := binary.LittleEndian.Uint64(data[tail-epochSize-sequenceSize : tail-epochSize])
	count := int(binary.LittleEndian.Uint32(data[n-core : n-core+entriesCountSize]))

	m := &Manifest{baseCount: count, nextSequence: nextSeq, epoch: epoch}

	var base []byte
	hasCount := false
	switch version {
	case bufferFooterVersion:
		base = data[:n-coreFooterSize]
	case snapshotFooterVersion, recordCountFooterVersion, checksumFooterVersion:
		if n < core+snapFixedSize {
			return nil, fmt.Errorf("wal: manifest too short for snapshot block (%d bytes)", n)
		}
		snapEnd := n - core
		createdStart := snapEnd - snapCreatedSize
		throughStart := createdStart - snapThroughSize
		locLenStart := throughStart - snapLocLenSize
		created := int64(binary.LittleEndian.Uint64(data[createdStart:snapEnd]))
		through := binary.LittleEndian.Uint64(data[throughStart:createdStart])
		locLen := int(binary.LittleEndian.Uint16(data[locLenStart:throughStart]))
		locStart := locLenStart - locLen
		if locStart < 0 {
			return nil, fmt.Errorf("wal: snapshot location length %d overruns manifest", locLen)
		}
		if locLen > 0 {
			m.snapshot = SnapshotPointer{Location: string(data[locStart:locLenStart]), ThroughSeq: through, CreatedUnixMs: created}
		} else {
			m.snapshot = SnapshotPointer{ThroughSeq: through, CreatedUnixMs: created}
		}
		base = data[:locStart]
		hasCount = version == recordCountFooterVersion || version == checksumFooterVersion
	default:
		return nil, fmt.Errorf("wal: unsupported manifest version %d", version)
	}

	if hasCount {
		m.base = base
		return m, nil
	}
	// Legacy: decode without a Count field and re-encode as v3 (Count==0).
	upgraded, err := upgradeLegacyEntries(base, count)
	if err != nil {
		return nil, err
	}
	m.base = upgraded
	return m, nil
}

// upgradeLegacyEntries decodes count legacy entries (no Count field) and
// re-encodes them in the v3 encoding with Count==0.
func upgradeLegacyEntries(base []byte, count int) ([]byte, error) {
	var out []byte
	off := 0
	for i := 0; i < count; i++ {
		e, next, err := decodeEntry(base, off, false)
		if err != nil {
			return nil, err
		}
		off = next
		out, err = encodeEntry(out, e) // e.Count == 0
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Bytes serializes the manifest in v3 form.
func (m *Manifest) Bytes() ([]byte, error) {
	total := m.baseCount + m.appendedCount
	if total > math.MaxUint32 {
		return nil, fmt.Errorf("wal: entry count %d exceeds u32 max", total)
	}
	loc := []byte(m.snapshot.Location)
	if len(loc) > math.MaxUint16 {
		return nil, fmt.Errorf("wal: snapshot location length %d exceeds u16 max", len(loc))
	}
	size := len(m.base) + len(m.appended) + len(loc) + snapFixedSize + coreFooterSizeV4
	buf := make([]byte, 0, size)
	buf = append(buf, m.base...)
	buf = append(buf, m.appended...)
	buf = append(buf, loc...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(loc)))
	buf = binary.LittleEndian.AppendUint64(buf, m.snapshot.ThroughSeq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.snapshot.CreatedUnixMs))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(total))
	buf = binary.LittleEndian.AppendUint64(buf, m.nextSequence)
	buf = binary.LittleEndian.AppendUint64(buf, m.epoch)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))
	buf = binary.LittleEndian.AppendUint16(buf, FooterVersion)
	return buf, nil
}

// Append assigns a record-sequence range to a new entry pointing at a segment
// object holding count framed records, and returns the sequence of the first
// record (the entry's base sequence). nextSequence advances by count. count
// must be >= 1.
func (m *Manifest) Append(location string, md []RecordMeta, count int) (uint64, error) {
	if count < 1 {
		return 0, fmt.Errorf("wal: Append count must be >= 1, got %d", count)
	}
	if count > math.MaxUint32 {
		return 0, fmt.Errorf("wal: Append count %d exceeds u32 max", count)
	}
	seq := m.nextSequence
	enc, err := encodeEntry(m.appended, Entry{Sequence: seq, Count: uint32(count), Location: location, Metadata: md})
	if err != nil {
		return 0, err
	}
	m.appended = enc
	m.appendedCount++
	m.nextSequence += uint64(count)
	return seq, nil
}

// SetSnapshot records a new base snapshot. The next Bytes/commit carries it.
func (m *Manifest) SetSnapshot(p SnapshotPointer) { m.snapshot = p }

// Snapshot returns the current snapshot pointer (zero value if none).
func (m *Manifest) Snapshot() SnapshotPointer { return m.snapshot }

// SetEpoch sets the fencing epoch (used by writer-fenced failover).
func (m *Manifest) SetEpoch(e uint64) { m.epoch = e }

// Epoch returns the current fencing epoch.
func (m *Manifest) Epoch() uint64 { return m.epoch }

// NextSequence returns the sequence the next appended record will receive.
func (m *Manifest) NextSequence() uint64 { return m.nextSequence }

// Count returns the number of live entries (base + appended).
func (m *Manifest) Count() int { return m.baseCount + m.appendedCount }

// Entries decodes and returns all live entries in sequence order.
func (m *Manifest) Entries() ([]Entry, error) {
	out := make([]Entry, 0, m.Count())
	for _, region := range [][]byte{m.base, m.appended} {
		off := 0
		for off < len(region) {
			e, next, err := decodeEntry(region, off, true)
			if err != nil {
				return nil, err
			}
			off = next
			out = append(out, e)
		}
	}
	return out, nil
}

// EntriesAfter returns live entries with Sequence > afterSeq, in order.
func (m *Manifest) EntriesAfter(afterSeq uint64) ([]Entry, error) {
	all, err := m.Entries()
	if err != nil {
		return nil, err
	}
	out := all[:0:0]
	for _, e := range all {
		if e.Sequence > afterSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// EntriesFrom returns live entries with Sequence >= minSeq, in order.
func (m *Manifest) EntriesFrom(minSeq uint64) ([]Entry, error) {
	all, err := m.Entries()
	if err != nil {
		return nil, err
	}
	out := all[:0:0]
	for _, e := range all {
		if e.Sequence >= minSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// EntriesContaining returns the entries needed to replay from record sequence
// seq onward: every entry whose range end is > seq, in order. The first entry
// returned is the one whose range contains seq (or, if seq is below all live
// entries, the earliest entry); a consumer skips records before seq in that
// first segment. This is the record-addressable read path.
func (m *Manifest) EntriesContaining(seq uint64) ([]Entry, error) {
	all, err := m.Entries()
	if err != nil {
		return nil, err
	}
	out := all[:0:0]
	for _, e := range all {
		if e.End() > seq {
			out = append(out, e)
		}
	}
	return out, nil
}

// Locate returns the entry whose record-sequence range contains seq, along with
// the index of that record within the segment (seq - entry.Sequence) and a
// found flag. A consumer reads only the manifest, calls Locate(seq) to learn
// which segment to fetch and how many leading records to skip, then streams
// forward. For a legacy (Count==0) entry the offset is always 0. Not found if
// seq is outside [firstLiveSequence, NextSequence).
func (m *Manifest) Locate(seq uint64) (Entry, int, bool) {
	all, err := m.Entries()
	if err != nil {
		return Entry{}, 0, false
	}
	// Entries are sorted and contiguous; binary search on Sequence.
	lo, hi := 0, len(all)
	for lo < hi {
		mid := (lo + hi) / 2
		if all[mid].End() <= seq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(all) && all[lo].Contains(seq) {
		return all[lo], int(seq - all[lo].Sequence), true
	}
	return Entry{}, 0, false
}

// TruncateThrough removes every entry whose records are all <= throughSeq (its
// End()-1 <= throughSeq), i.e. entries fully superseded by a snapshot covering
// up to throughSeq. An entry with any record > throughSeq is kept whole (a
// segment is never split). Returns the number of entries removed.
func (m *Manifest) TruncateThrough(throughSeq uint64) (int, error) {
	all, err := m.Entries()
	if err != nil {
		return 0, err
	}
	kept := all[:0:0]
	removed := 0
	for _, e := range all {
		if e.End()-1 <= throughSeq {
			removed++
			continue
		}
		kept = append(kept, e)
	}
	if removed == 0 {
		return 0, nil
	}
	var buf []byte
	for _, e := range kept {
		buf, err = encodeEntry(buf, e)
		if err != nil {
			return 0, err
		}
	}
	m.base = buf
	m.baseCount = len(kept)
	m.appended = nil
	m.appendedCount = 0
	return removed, nil
}

func encodeEntry(buf []byte, e Entry) ([]byte, error) {
	metadataSize := metadataCountSize
	for _, md := range e.Metadata {
		if len(md.Payload) > math.MaxUint32 {
			return nil, fmt.Errorf("wal: metadata payload %d exceeds u32 max", len(md.Payload))
		}
		metadataSize += startIndexSize + ingestionTimeSize + metadataLenSize + len(md.Payload)
	}
	if len(e.Location) > math.MaxUint16 {
		return nil, fmt.Errorf("wal: location length %d exceeds u16 max", len(e.Location))
	}
	bodyLen := sequenceSize + countFieldSize + locationLenSize + len(e.Location) + metadataSize
	if bodyLen > math.MaxUint32 {
		return nil, fmt.Errorf("wal: entry body length %d exceeds u32 max", bodyLen)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(bodyLen))
	buf = binary.LittleEndian.AppendUint64(buf, e.Sequence)
	buf = binary.LittleEndian.AppendUint32(buf, e.Count)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.Location)))
	buf = append(buf, e.Location...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Metadata)))
	for _, md := range e.Metadata {
		buf = binary.LittleEndian.AppendUint32(buf, md.StartIndex)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(md.IngestionTimeMs))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(md.Payload)))
		buf = append(buf, md.Payload...)
	}
	return buf, nil
}

// decodeEntry decodes one entry. hasCount selects the v3 encoding (with the
// Count field) vs the legacy encoding (no Count, yielding Count==0).
func decodeEntry(data []byte, offset int, hasCount bool) (Entry, int, error) {
	fail := func(msg string) (Entry, int, error) {
		return Entry{}, 0, fmt.Errorf("wal: %s", msg)
	}
	if offset+entryBodyLenSize > len(data) {
		return fail("truncated entry body length")
	}
	bodyLen := int(binary.LittleEndian.Uint32(data[offset : offset+entryBodyLenSize]))
	off := offset + entryBodyLenSize
	end := off + bodyLen
	minBody := sequenceSize + locationLenSize + metadataCountSize
	if hasCount {
		minBody += countFieldSize
	}
	if bodyLen < minBody || end > len(data) {
		return fail("entry body overruns manifest")
	}
	seq := binary.LittleEndian.Uint64(data[off : off+sequenceSize])
	off += sequenceSize
	var cnt uint32
	if hasCount {
		cnt = binary.LittleEndian.Uint32(data[off : off+countFieldSize])
		off += countFieldSize
	}
	locLen := int(binary.LittleEndian.Uint16(data[off : off+locationLenSize]))
	off += locationLenSize
	if off+locLen > end {
		return fail("entry location overruns body")
	}
	location := string(data[off : off+locLen])
	off += locLen
	if off+metadataCountSize > end {
		return fail("truncated metadata count")
	}
	mdCount := int(binary.LittleEndian.Uint32(data[off : off+metadataCountSize]))
	off += metadataCountSize
	var md []RecordMeta
	if mdCount > 0 {
		md = make([]RecordMeta, 0, mdCount)
	}
	for i := 0; i < mdCount; i++ {
		if off+startIndexSize+ingestionTimeSize+metadataLenSize > end {
			return fail("truncated metadata header")
		}
		start := binary.LittleEndian.Uint32(data[off : off+startIndexSize])
		off += startIndexSize
		ingMs := int64(binary.LittleEndian.Uint64(data[off : off+ingestionTimeSize]))
		off += ingestionTimeSize
		plen := int(binary.LittleEndian.Uint32(data[off : off+metadataLenSize]))
		off += metadataLenSize
		if off+plen > end {
			return fail("metadata payload overruns body")
		}
		payload := make([]byte, plen)
		copy(payload, data[off:off+plen])
		off += plen
		md = append(md, RecordMeta{StartIndex: start, IngestionTimeMs: ingMs, Payload: payload})
	}
	if off != end {
		return fail("entry body has trailing bytes")
	}
	return Entry{Sequence: seq, Count: cnt, Location: location, Metadata: md}, end, nil
}

// TailLocations maps the last `limit` live entries by segment location.
// Locations are unique per (runID, ordinal), so a writer that lost a CAS
// response can check whether its entries landed. Only the tail is scanned:
// those entries are necessarily last.
func (m *Manifest) TailLocations(limit int) (map[string]Entry, error) {
	if limit <= 0 {
		return nil, nil
	}
	all, err := m.Entries()
	if err != nil {
		return nil, err
	}
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	out := make(map[string]Entry, len(all))
	for _, e := range all {
		out[e.Location] = e
	}
	return out, nil
}
