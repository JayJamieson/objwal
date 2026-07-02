package kv

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tempLocal(t *testing.T) *local {
	t.Helper()
	l, err := openLocal(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	t.Cleanup(func() { l.close() })
	return l
}

func mustGet(t *testing.T, l *local, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := l.get([]byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return v, ok
}

func TestLocalPutGet(t *testing.T) {
	l := tempLocal(t)
	if err := l.apply(opPut, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	v, ok := mustGet(t, l, "k")
	if !ok || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("get = %q,%v; want \"v\",true", v, ok)
	}
}

func TestLocalGetMissing(t *testing.T) {
	l := tempLocal(t)
	if _, ok := mustGet(t, l, "nope"); ok {
		t.Fatal("expected miss for absent key")
	}
}

func TestLocalOverwrite(t *testing.T) {
	l := tempLocal(t)
	_ = l.apply(opPut, []byte("k"), []byte("first"))
	_ = l.apply(opPut, []byte("k"), []byte("second"))
	v, ok := mustGet(t, l, "k")
	if !ok || !bytes.Equal(v, []byte("second")) {
		t.Fatalf("get = %q,%v; want \"second\",true", v, ok)
	}
}

func TestLocalDelete(t *testing.T) {
	l := tempLocal(t)
	_ = l.apply(opPut, []byte("k"), []byte("v"))
	_ = l.apply(opDelete, []byte("k"), nil)
	if _, ok := mustGet(t, l, "k"); ok {
		t.Fatal("expected miss after delete")
	}
}

func TestLocalRecoversOnReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	l, err := openLocal(path)
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	_ = l.apply(opPut, []byte("a"), []byte("1"))
	_ = l.apply(opPut, []byte("b"), []byte("2"))
	_ = l.apply(opPut, []byte("a"), []byte("3")) // overwrite a
	_ = l.apply(opDelete, []byte("b"), nil)      // delete b
	l.close()

	l2, err := openLocal(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.close()

	if v, ok := mustGet(t, l2, "a"); !ok || !bytes.Equal(v, []byte("3")) {
		t.Errorf("a = %q,%v; want \"3\",true", v, ok)
	}
	if _, ok := mustGet(t, l2, "b"); ok {
		t.Error("b should be absent after replayed delete")
	}
}

func TestLocalTruncatesFromFirstCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	l, err := openLocal(path)
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	_ = l.apply(opPut, []byte("k1"), []byte("v1"))
	k2Start := l.tail // start of the second record
	_ = l.apply(opPut, []byte("k2"), []byte("v2"))
	_ = l.apply(opPut, []byte("k3"), []byte("v3"))
	l.close()

	// Corrupt a body byte of the second record (the op byte at recStart+recLenSize)
	// so its stored CRC no longer matches. A length-prefixed log can't resync past
	// a corrupt record, so the scan must drop k2 AND everything after it (k3).
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, k2Start+recLenSize); err != nil {
		t.Fatalf("read byte to corrupt: %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, k2Start+recLenSize); err != nil {
		t.Fatalf("write corrupt byte: %v", err)
	}
	f.Close()

	l2, err := openLocal(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.close()

	if v, ok := mustGet(t, l2, "k1"); !ok || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("k1 = %q,%v; want \"v1\",true", v, ok)
	}
	if _, ok := mustGet(t, l2, "k2"); ok {
		t.Error("k2 (corrupt) should have been dropped")
	}
	if _, ok := mustGet(t, l2, "k3"); ok {
		t.Error("k3 (after corruption) should have been dropped too")
	}
	if l2.tail != k2Start {
		t.Errorf("tail = %d; want %d (truncated at first corrupt record)", l2.tail, k2Start)
	}
}

func TestLocalDropsTornTailOnReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	l, err := openLocal(path)
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	_ = l.apply(opPut, []byte("good"), []byte("v1"))
	goodEnd := l.tail // end of the first, complete record
	_ = l.apply(opPut, []byte("torn"), []byte("v2"))
	l.close()

	// Simulate a crash mid-write: chop into the middle of the second record so
	// its declared length runs past EOF.
	if err := os.Truncate(path, goodEnd+3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	l2, err := openLocal(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.close()

	if v, ok := mustGet(t, l2, "good"); !ok || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("good = %q,%v; want \"v1\",true", v, ok)
	}
	if _, ok := mustGet(t, l2, "torn"); ok {
		t.Error("torn record should have been dropped")
	}
	if l2.tail != goodEnd {
		t.Errorf("tail = %d after dropping torn tail; want %d", l2.tail, goodEnd)
	}
}
