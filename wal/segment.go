package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/klauspost/compress/zstd"
)

// Segment wire format.
//
// v2 (written by default):
//
//	[ record block: repeated [len: u32 LE][bytes], optionally zstd ]
//	[ compression_type : u8     ]
//	[ record_count     : u32 LE ]
//	[ crc32c           : u32 LE ]  <- covers everything above
//	[ version (= 2)    : u16 LE ]
//
// v1 (the buffer's data batch v1, still read, and still written when
// ProducerConfig.LegacySegmentFormat is set):
//
//	[ record block ][ compression_type: u8 ][ record_count: u32 LE ][ version (=1): u16 LE ]
//
// The checksum covers the block as stored plus the compression and count
// fields, so a corrupt object is rejected before any zstd work. Only the
// version field is outside the digest; it is validated structurally.
//
// Records are opaque []byte; their meaning is the Applier's concern.

// ErrCorrupt reports that stored bytes failed their integrity check. Distinct
// from a transport error: retrying the GET re-reads the same bad bytes, so
// replication should stop rather than spin.
var ErrCorrupt = errors.New("wal: checksum mismatch (corrupt object)")

// Compression selects the codec applied to a segment's record block.
type Compression uint8

const (
	CompressionNone Compression = 0
	CompressionZstd Compression = 1
)

const (
	segmentFormatVersion uint16 = 2
	segmentLegacyVersion uint16 = 1
	recordLenSize               = 4
	segmentCRCSize              = 4
	segmentFooterSizeV1         = 1 + 4 + 2     // comp u8 + count u32 + version u16
	segmentFooterSize           = 1 + 4 + 4 + 2 // + crc32c u32
)

// CRC-32C: hardware-accelerated on amd64/arm64 and stronger than IEEE for
// short-burst errors.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// encodeSegment serializes opaque records into the segment wire format.
// legacy selects the v1 (buffer-compatible, unchecksummed) framing.
func encodeSegment(records [][]byte, comp Compression, legacy bool) ([]byte, error) {
	if len(records) > math.MaxUint32 {
		return nil, fmt.Errorf("wal: record count %d exceeds u32 max", len(records))
	}
	blockSize := 0
	for _, r := range records {
		if len(r) > math.MaxUint32 {
			return nil, fmt.Errorf("wal: record size %d exceeds u32 max", len(r))
		}
		blockSize += recordLenSize + len(r)
	}
	block := make([]byte, 0, blockSize)
	var lenBuf [4]byte
	for _, r := range records {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(r)))
		block = append(block, lenBuf[:]...)
		block = append(block, r...)
	}
	if comp == CompressionZstd {
		block = zstdEncoder.EncodeAll(block, make([]byte, 0, len(block)/2))
	}
	out := make([]byte, 0, len(block)+segmentFooterSize)
	out = append(out, block...)
	out = append(out, byte(comp))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(records)))
	if legacy {
		return binary.LittleEndian.AppendUint16(out, segmentLegacyVersion), nil
	}
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
	return binary.LittleEndian.AppendUint16(out, segmentFormatVersion), nil
}

// decodeSegment parses the segment wire format back into its opaque records,
// verifying the checksum when the object carries one.
func decodeSegment(data []byte) ([][]byte, error) {
	if len(data) < segmentFooterSizeV1 {
		return nil, fmt.Errorf("wal: segment too small for footer")
	}
	n := len(data)
	version := binary.LittleEndian.Uint16(data[n-2:])

	var body []byte
	var comp Compression
	var count uint32
	switch version {
	case segmentFormatVersion:
		if n < segmentFooterSize {
			return nil, fmt.Errorf("wal: segment too small for v2 footer")
		}
		want := binary.LittleEndian.Uint32(data[n-6 : n-2])
		covered := data[:n-6]
		if got := crc32.Checksum(covered, castagnoli); got != want {
			return nil, fmt.Errorf("%w: segment crc32c %08x, stored %08x", ErrCorrupt, got, want)
		}
		comp = Compression(data[n-11])
		count = binary.LittleEndian.Uint32(data[n-10 : n-6])
		body = data[: n-segmentFooterSize : n-segmentFooterSize]
	case segmentLegacyVersion:
		comp = Compression(data[n-7])
		count = binary.LittleEndian.Uint32(data[n-6 : n-2])
		body = data[: n-segmentFooterSizeV1 : n-segmentFooterSizeV1]
	default:
		return nil, fmt.Errorf("wal: unsupported segment version %d", version)
	}

	if comp != CompressionNone && comp != CompressionZstd {
		return nil, fmt.Errorf("wal: unsupported compression %d", comp)
	}
	if comp == CompressionZstd {
		var err error
		body, err = zstdDecoder.DecodeAll(body, nil)
		if err != nil {
			return nil, fmt.Errorf("wal: zstd decode failed: %w", err)
		}
	}
	records := make([][]byte, 0, count)
	off := 0
	for i := uint32(0); i < count; i++ {
		if len(body)-off < recordLenSize {
			return nil, fmt.Errorf("wal: truncated record length")
		}
		r := int(binary.LittleEndian.Uint32(body[off : off+recordLenSize]))
		off += recordLenSize
		if len(body)-off < r {
			return nil, fmt.Errorf("wal: truncated record data")
		}
		records = append(records, body[off:off+r:off+r])
		off += r
	}
	return records, nil
}
