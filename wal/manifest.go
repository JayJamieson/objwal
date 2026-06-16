// Package wal turns the buffer's queue primitives into a replication log.
//
// The buffer is a destructive single-consumer queue; replication needs a
// non-destructively *tailed* log with a base snapshot for bootstrap. This
// package reuses the buffer's object-store abstraction and append-friendly
// manifest, and extends the manifest with a snapshot pointer (footer v2) so a
// single CAS-protected object describes both the live WAL tail and the base a
// new or lagging replica should restore from.
//
//   - Writer (primary): on each write, frames a record and (a) appends it to
//     its local store and (b) hands it to a buffer-style producer that batches
//     records into segment objects and CAS-appends entries to this manifest.
//     The writer is fenced by epoch: a new primary bumps epoch, a stale
//     primary's CAS appends start failing and it steps down.
//   - Reader (replica): restores the snapshot at Snapshot().ThroughSeq, then
//     polls this manifest and applies entries with Sequence > its cursor.
//   - Snapshot/GC: periodically uploads a base snapshot, records it here via
//     SetSnapshot, TruncateThrough-es superseded entries out of the manifest,
//     and lets a time/size policy delete the orphaned segment objects.
package wal

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Footer/format constants. Entry encoding is byte-identical to the buffer's
// v1 manifest entries; only the footer differs (v2 carries a snapshot block).
const (
	// FooterVersion is the version this package writes.
	FooterVersion uint16 = 2
	// bufferFooterVersion is the buffer's v1 footer, which this package can
	// read (a v1 manifest is simply a v2 manifest with no snapshot block).
	bufferFooterVersion uint16 = 1

	entriesCountSize = 4
	sequenceSize     = 8
	epochSize        = 8
	versionSize      = 2
	// coreFooter is the trailing block shared by v1 and v2, at identical
	// offsets from the end of the object.
	coreFooterSize = entriesCountSize + sequenceSize + epochSize + versionSize // 22

	snapLocLenSize  = 2
	snapThroughSize = 8
	snapCreatedSize = 8
	snapFixedSize   = snapLocLenSize + snapThroughSize + snapCreatedSize // 18

	locationLenSize   = 2
	metadataCountSize = 4
	startIndexSize    = 4
	ingestionTimeSize = 8
	metadataLenSize   = 4
	entryBodyLenSize  = 4
)

// SnapshotPointer identifies the base snapshot a replica bootstraps from.
// The zero value (empty Location) means "no snapshot has been taken yet".
type SnapshotPointer struct {
	// Location is the object path of the snapshot. Empty means none.
	Location string
	// ThroughSeq is the highest WAL sequence included in the snapshot. A
	// replica restores the snapshot, sets its cursor to ThroughSeq, and
	// applies only entries with Sequence > ThroughSeq.
	ThroughSeq uint64
	// CreatedUnixMs is the snapshot's wall-clock creation time, used by the
	// retention policy.
	CreatedUnixMs int64
}

// IsZero reports whether no snapshot has been recorded.
func (s SnapshotPointer) IsZero() bool { return s.Location == "" }

// RecordMeta is a per-range annotation attached to a manifest entry,
// delimiting the run of framed records beginning at StartIndex. It mirrors the
// buffer's Metadata so segment objects and manifests stay tooling-compatible.
type RecordMeta struct {
	StartIndex      uint32
	IngestionTimeMs int64
	Payload         []byte
}

// Entry is one committed manifest entry: a segment object plus the metadata
// ranges describing the framed records it holds.
type Entry struct {
	Sequence uint64
	Location string
	Metadata []RecordMeta
}

// Manifest is the in-memory, mutable view of a replication-log manifest. It
// preserves the buffer's O(1) append shape: existing entries are held as raw
// bytes (base) and new entries accumulate in a side buffer (appended); only a
// snapshot change or truncation rewrites base.
//
// Manifest is not safe for concurrent mutation; the writer owns one instance.
type Manifest struct {
	base          []byte // encoded existing entries (no snapshot block, no footer)
	baseCount     int
	appended      []byte // encoded newly appended entries
	appendedCount int
	nextSequence  uint64
	epoch         uint64
	snapshot      SnapshotPointer
}

// NewManifest returns an empty manifest at epoch 0 with no snapshot.
func NewManifest() *Manifest { return &Manifest{} }

// ParseManifest decodes a serialized manifest. It accepts both v2 (this
// package) and v1 (the buffer) footers; a v1 manifest yields a zero snapshot.
func ParseManifest(data []byte) (*Manifest, error) {
	if len(data) < coreFooterSize {
		return nil, fmt.Errorf("wal: manifest too short for footer (%d bytes)", len(data))
	}
	n := len(data)
	version := binary.LittleEndian.Uint16(data[n-versionSize:])
	// Core footer sits at identical offsets from the end in v1 and v2.
	epoch := binary.LittleEndian.Uint64(data[n-versionSize-epochSize : n-versionSize])
	nextSeq := binary.LittleEndian.Uint64(data[n-versionSize-epochSize-sequenceSize : n-versionSize-epochSize])
	count := binary.LittleEndian.Uint32(data[n-coreFooterSize : n-coreFooterSize+entriesCountSize])

	m := &Manifest{baseCount: int(count), nextSequence: nextSeq, epoch: epoch}

	switch version {
	case bufferFooterVersion:
		m.base = data[:n-coreFooterSize]
	case FooterVersion:
		if n < coreFooterSize+snapFixedSize {
			return nil, fmt.Errorf("wal: manifest too short for v2 snapshot block (%d bytes)", n)
		}
		snapEnd := n - coreFooterSize // snapshot block ends where the core footer begins
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
			m.snapshot = SnapshotPointer{
				Location:      string(data[locStart:locLenStart]),
				ThroughSeq:    through,
				CreatedUnixMs: created,
			}
		} else {
			m.snapshot = SnapshotPointer{ThroughSeq: through, CreatedUnixMs: created}
			m.snapshot.Location = "" // explicit: no snapshot
		}
		m.base = data[:locStart]
	default:
		return nil, fmt.Errorf("wal: unsupported manifest version %d", version)
	}
	return m, nil
}

// Bytes serializes the manifest in v2 form.
func (m *Manifest) Bytes() ([]byte, error) {
	total := m.baseCount + m.appendedCount
	if total > math.MaxUint32 {
		return nil, fmt.Errorf("wal: entry count %d exceeds u32 max", total)
	}
	loc := []byte(m.snapshot.Location)
	if len(loc) > math.MaxUint16 {
		return nil, fmt.Errorf("wal: snapshot location length %d exceeds u16 max", len(loc))
	}
	size := len(m.base) + len(m.appended) + len(loc) + snapFixedSize + coreFooterSize
	buf := make([]byte, 0, size)
	buf = append(buf, m.base...)
	buf = append(buf, m.appended...)
	// snapshot block
	buf = append(buf, loc...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(loc)))
	buf = binary.LittleEndian.AppendUint64(buf, m.snapshot.ThroughSeq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.snapshot.CreatedUnixMs))
	// core footer (matches buffer v1 field order)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(total))
	buf = binary.LittleEndian.AppendUint64(buf, m.nextSequence)
	buf = binary.LittleEndian.AppendUint64(buf, m.epoch)
	buf = binary.LittleEndian.AppendUint16(buf, FooterVersion)
	return buf, nil
}

// Append assigns the next sequence to a new entry pointing at a segment object
// and returns the assigned sequence. The append is O(1): it touches only the
// side buffer, not base or the snapshot block.
func (m *Manifest) Append(location string, md []RecordMeta) (uint64, error) {
	seq := m.nextSequence
	enc, err := encodeEntry(m.appended, Entry{Sequence: seq, Location: location, Metadata: md})
	if err != nil {
		return 0, err
	}
	m.appended = enc
	m.appendedCount++
	m.nextSequence++
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

// NextSequence returns the sequence the next Append will assign.
func (m *Manifest) NextSequence() uint64 { return m.nextSequence }

// Count returns the number of live entries (base + appended).
func (m *Manifest) Count() int { return m.baseCount + m.appendedCount }

// Entries decodes and returns all live entries in sequence order.
func (m *Manifest) Entries() ([]Entry, error) { return m.entriesFrom(0, false) }

// EntriesAfter returns live entries with Sequence > afterSeq, in order.
func (m *Manifest) EntriesAfter(afterSeq uint64) ([]Entry, error) {
	return m.entriesFrom(afterSeq, true)
}

// EntriesFrom returns live entries with Sequence >= minSeq, in order. This is
// the tailer's read path: the replica's half-open cursor is the next sequence
// it expects to apply, so minSeq == cursor delivers everything not yet seen,
// including sequence 0.
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

func (m *Manifest) entriesFrom(afterSeq uint64, filter bool) ([]Entry, error) {
	out := make([]Entry, 0, m.Count())
	for _, region := range [][]byte{m.base, m.appended} {
		off := 0
		for off < len(region) {
			e, next, err := decodeEntry(region, off)
			if err != nil {
				return nil, err
			}
			off = next
			if !filter || e.Sequence > afterSeq {
				out = append(out, e)
			}
		}
	}
	return out, nil
}

// TruncateThrough removes all entries with Sequence <= throughSeq from the
// front of the manifest. It is the manifest-side companion to a snapshot: once
// a snapshot covers up to throughSeq, those entries are redundant (a replica
// bootstraps from the snapshot instead), and their segment objects become
// eligible for retention GC. Returns the number of entries removed.
//
// Truncation is rare (snapshot cadence), so this re-encodes the survivors
// rather than slicing; correctness over micro-optimization.
func (m *Manifest) TruncateThrough(throughSeq uint64) (int, error) {
	all, err := m.Entries()
	if err != nil {
		return 0, err
	}
	kept := all[:0:0]
	removed := 0
	for _, e := range all {
		if e.Sequence <= throughSeq {
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
	bodyLen := sequenceSize + locationLenSize + len(e.Location) + metadataSize
	if bodyLen > math.MaxUint32 {
		return nil, fmt.Errorf("wal: entry body length %d exceeds u32 max", bodyLen)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(bodyLen))
	buf = binary.LittleEndian.AppendUint64(buf, e.Sequence)
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

func decodeEntry(data []byte, offset int) (Entry, int, error) {
	fail := func(msg string) (Entry, int, error) {
		return Entry{}, 0, fmt.Errorf("wal: %s", msg)
	}
	if offset+entryBodyLenSize > len(data) {
		return fail("truncated entry body length")
	}
	bodyLen := int(binary.LittleEndian.Uint32(data[offset : offset+entryBodyLenSize]))
	off := offset + entryBodyLenSize
	end := off + bodyLen
	if bodyLen < sequenceSize+locationLenSize+metadataCountSize || end > len(data) {
		return fail("entry body overruns manifest")
	}
	seq := binary.LittleEndian.Uint64(data[off : off+sequenceSize])
	off += sequenceSize
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
	return Entry{Sequence: seq, Location: location, Metadata: md}, end, nil
}
