package kv

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Local file record framing (see the design spec):
//
//	[recLen u32]  bytes that follow, up to and including crc
//	[op    u8]    opPut | opDelete
//	[klen  u32]
//	[key   klen bytes]
//	[value vlen bytes]   vlen = recLen - 1 - 4 - klen - 4   (0 for delete)
//	[crc32 u32]   CRC-32 (IEEE) over op..value
//
// The keydir stores the absolute offset and length of the [value] region, so a
// read is a single ReadAt. All integers are little-endian.

const (
	recLenSize  = 4
	recCRCSize  = 4
	recHdrFixed = 1 + 4 // op u8 + klen u32
	// minRecLen is the smallest legal recLen: op + klen + crc (empty key/value).
	minRecLen = recHdrFixed + recCRCSize
)

// loc is the keydir entry: where a key's value bytes live in the local file.
type loc struct {
	off int64
	len uint32
}

// local is the bitcask-style read/recovery core shared by the primary and the
// replica: an in-memory keydir over an append-only file. apply mutates it
// (under a write lock); get reads it (under a read lock).
type local struct {
	mu     sync.RWMutex
	keydir map[string]loc
	f      *os.File
	tail   int64 // current end-of-file (and append) offset
	path   string
}

// openLocal opens (creating if absent) the append-only file at path and rebuilds
// the keydir by scanning it. A torn or corrupt trailing record is truncated away
// so the file is left at a clean record boundary ready for appends.
func openLocal(path string) (*local, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kv: open local file: %w", err)
	}
	l := &local{keydir: make(map[string]loc), f: f, path: path}
	if err := l.scan(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// scan reads the file front-to-back, rebuilding the keydir and setting tail to
// the end of the last intact record. It stops at the FIRST record that fails to
// validate (short read or bad CRC) and truncates everything from there: a
// length-prefixed log cannot reliably resync past a corrupt record, so a torn
// tail and interior corruption are handled identically — the remainder is
// dropped. For the no-fsync demo the expected case is a torn tail from a crash
// mid-append.
func (l *local) scan() error {
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("kv: seek local file: %w", err)
	}
	data, err := io.ReadAll(l.f)
	if err != nil {
		return fmt.Errorf("kv: read local file: %w", err)
	}

	off := 0
	for {
		if len(data)-off < recLenSize {
			break // no room for a length header: clean EOF or torn tail
		}
		recLen := int(binary.LittleEndian.Uint32(data[off : off+recLenSize]))
		payloadStart := off + recLenSize
		if recLen < minRecLen || len(data)-payloadStart < recLen {
			break // declared length runs past EOF, or is impossibly small: torn tail
		}
		payload := data[payloadStart : payloadStart+recLen]
		body := payload[:len(payload)-recCRCSize]
		wantCRC := binary.LittleEndian.Uint32(payload[len(payload)-recCRCSize:])
		if crc32.ChecksumIEEE(body) != wantCRC {
			break // corrupt record: stop and treat the rest as garbage
		}
		op := body[0]
		klen := int(binary.LittleEndian.Uint32(body[1:5]))
		if len(body)-recHdrFixed < klen {
			break // malformed key length
		}
		key := body[recHdrFixed : recHdrFixed+klen]
		value := body[recHdrFixed+klen:]
		switch op {
		case opPut:
			l.keydir[string(key)] = loc{
				off: int64(payloadStart + recHdrFixed + klen),
				len: uint32(len(value)),
			}
		case opDelete:
			delete(l.keydir, string(key))
		default:
			// A CRC-valid record whose op is neither put nor delete cannot be a
			// torn write — the bytes are intact as written, and apply only ever
			// writes put/delete. It means a format/version mismatch, so fail loud
			// rather than silently discarding intact data (unlike a CRC failure,
			// which is a recoverable torn tail handled by the break above).
			return fmt.Errorf("kv: unknown op %d in local file at offset %d", op, off)
		}
		off = payloadStart + recLen
	}

	l.tail = int64(off)
	if int64(len(data)) != l.tail {
		if err := l.f.Truncate(l.tail); err != nil {
			return fmt.Errorf("kv: truncate torn tail: %w", err)
		}
	}
	return nil
}

// isEmpty reports whether the local file holds no records (so the caller can
// decide to rebuild from objwal instead).
func (l *local) isEmpty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tail == 0
}

// apply appends a record for the mutation and updates the keydir. It is the one
// write path shared by the primary's Put/Delete and the replica's Applier.
func (l *local) apply(op byte, key, value []byte) error {
	body := make([]byte, 0, recHdrFixed+len(key)+len(value))
	body = append(body, op)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(key)))
	body = append(body, key...)
	body = append(body, value...)

	recLen := len(body) + recCRCSize
	rec := make([]byte, 0, recLenSize+recLen)
	rec = binary.LittleEndian.AppendUint32(rec, uint32(recLen))
	rec = append(rec, body...)
	rec = binary.LittleEndian.AppendUint32(rec, crc32.ChecksumIEEE(body))

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.WriteAt(rec, l.tail); err != nil {
		return fmt.Errorf("kv: append local record: %w", err)
	}
	valueOff := l.tail + recLenSize + recHdrFixed + int64(len(key))
	switch op {
	case opPut:
		l.keydir[string(key)] = loc{off: valueOff, len: uint32(len(value))}
	case opDelete:
		delete(l.keydir, string(key))
	}
	l.tail += int64(len(rec))
	return nil
}

// get returns the value for key. found is false (with nil value and error) when
// the key is absent or has been deleted.
func (l *local) get(key []byte) (value []byte, found bool, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e, ok := l.keydir[string(key)]
	if !ok {
		return nil, false, nil
	}
	buf := make([]byte, e.len)
	if _, err := l.f.ReadAt(buf, e.off); err != nil {
		return nil, false, fmt.Errorf("kv: read value: %w", err)
	}
	return buf, true, nil
}

// close closes the underlying file.
func (l *local) close() error {
	return l.f.Close()
}
