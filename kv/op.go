package kv

import (
	"encoding/binary"
	"fmt"
)

// op identifies the kind of mutation carried by a record.
const (
	opPut    byte = 0
	opDelete byte = 1
)

// encodeOp serializes a mutation into kv's opaque objwal record frame:
//
//	[op u8][klen u32][key][value]
//
// value is the remaining bytes after the key (empty for a delete). All integers
// are little-endian, matching the rest of the repo.
func encodeOp(op byte, key, value []byte) []byte {
	frame := make([]byte, 0, 1+4+len(key)+len(value))
	frame = append(frame, op)
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(key)))
	frame = append(frame, key...)
	frame = append(frame, value...)
	return frame
}

// decodeOp parses a frame produced by encodeOp. The returned key and value
// alias the input frame; copy them if they must outlive it.
func decodeOp(frame []byte) (op byte, key, value []byte, err error) {
	if len(frame) < 1+4 {
		return 0, nil, nil, fmt.Errorf("kv: op frame too short (%d bytes)", len(frame))
	}
	op = frame[0]
	klen := int(binary.LittleEndian.Uint32(frame[1:5]))
	if len(frame)-5 < klen {
		return 0, nil, nil, fmt.Errorf("kv: op frame key length %d exceeds frame", klen)
	}
	key = frame[5 : 5+klen]
	value = frame[5+klen:]
	return op, key, value, nil
}
