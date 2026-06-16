package wal

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
)

// Segment wire format is byte-compatible with the buffer's data batch v1, so
// segment objects remain inspectable by the same tooling:
//
//	[ record block: repeated [len: u32 LE][bytes] ]
//	[ compression_type : u8     ]
//	[ record_count     : u32 LE ]
//	[ version (= 1)    : u16 LE ]
//
// Records are opaque []byte. This package attaches no meaning to their
// contents; that is the Applier's concern.

// Compression selects the codec applied to a segment's record block.
type Compression uint8

const (
	CompressionNone Compression = 0
	CompressionZstd Compression = 1
)

const (
	segmentFormatVersion uint16 = 1
	recordLenSize               = 4
	segmentFooterSize           = 1 + 4 + 2 // compression u8 + count u32 + version u16
)

var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// encodeSegment serializes opaque records into the segment wire format.
func encodeSegment(records [][]byte, comp Compression) ([]byte, error) {
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
	out = binary.LittleEndian.AppendUint16(out, segmentFormatVersion)
	return out, nil
}

// decodeSegment parses the segment wire format back into its opaque records.
func decodeSegment(data []byte) ([][]byte, error) {
	if len(data) < segmentFooterSize {
		return nil, fmt.Errorf("wal: segment too small for footer")
	}
	footer := data[len(data)-segmentFooterSize:]
	body := data[:len(data)-segmentFooterSize]

	comp := Compression(footer[0])
	count := binary.LittleEndian.Uint32(footer[1:5])
	version := binary.LittleEndian.Uint16(footer[5:7])
	if version != segmentFormatVersion {
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
		n := int(binary.LittleEndian.Uint32(body[off : off+recordLenSize]))
		off += recordLenSize
		if len(body)-off < n {
			return nil, fmt.Errorf("wal: truncated record data")
		}
		records = append(records, body[off:off+n:off+n])
		off += n
	}
	return records, nil
}
