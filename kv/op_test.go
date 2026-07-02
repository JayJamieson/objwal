package kv

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeOpPut(t *testing.T) {
	frame := encodeOp(opPut, []byte("hello"), []byte("world"))

	op, key, value, err := decodeOp(frame)
	if err != nil {
		t.Fatalf("decodeOp: %v", err)
	}
	if op != opPut {
		t.Errorf("op = %d, want %d", op, opPut)
	}
	if !bytes.Equal(key, []byte("hello")) {
		t.Errorf("key = %q, want %q", key, "hello")
	}
	if !bytes.Equal(value, []byte("world")) {
		t.Errorf("value = %q, want %q", value, "world")
	}
}

func TestEncodeDecodeOpDelete(t *testing.T) {
	frame := encodeOp(opDelete, []byte("hello"), nil)

	op, key, value, err := decodeOp(frame)
	if err != nil {
		t.Fatalf("decodeOp: %v", err)
	}
	if op != opDelete {
		t.Errorf("op = %d, want %d", op, opDelete)
	}
	if !bytes.Equal(key, []byte("hello")) {
		t.Errorf("key = %q, want %q", key, "hello")
	}
	if len(value) != 0 {
		t.Errorf("value = %q, want empty", value)
	}
}

func TestEncodeDecodeOpBinaryKeyValue(t *testing.T) {
	key := []byte{0x00, 0xff, 0x01, 0x80}
	value := []byte{0xde, 0xad, 0x00, 0xbe, 0xef}

	op, gotKey, gotValue, err := decodeOp(encodeOp(opPut, key, value))
	if err != nil {
		t.Fatalf("decodeOp: %v", err)
	}
	if op != opPut || !bytes.Equal(gotKey, key) || !bytes.Equal(gotValue, value) {
		t.Errorf("round-trip mismatch: op=%d key=%x value=%x", op, gotKey, gotValue)
	}
}

func TestDecodeOpTruncated(t *testing.T) {
	if _, _, _, err := decodeOp([]byte{0x00, 0x01}); err == nil {
		t.Fatal("expected error decoding a truncated frame, got nil")
	}
}
