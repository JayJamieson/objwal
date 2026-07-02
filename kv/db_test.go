package kv

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

func testConfig(dir string) Config {
	return Config{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/seg",
		LocalPath:     filepath.Join(dir, "data"),
		FlushInterval: 5 * time.Millisecond,
	}
}

func dbGet(t *testing.T, db *DB, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	return v, ok
}

func TestDBPutGet(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()
	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close(ctx)

	if err := db.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, ok := dbGet(t, db, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v; want \"v\",true", v, ok)
	}
}

func TestDBDelete(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()
	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close(ctx)

	_ = db.Put(ctx, []byte("k"), []byte("v"))
	if err := db.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := dbGet(t, db, "k"); ok {
		t.Fatal("expected miss after delete")
	}
}

func TestDBReopenRecoversFromLocalFile(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()
	dir := t.TempDir()

	db, err := Open(ctx, store, testConfig(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = db.Put(ctx, []byte("a"), []byte("1"))
	_ = db.Put(ctx, []byte("b"), []byte("2"))
	if err := db.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen against the same local file (and store).
	db2, err := Open(ctx, store, testConfig(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close(ctx)

	if v, ok := dbGet(t, db2, "a"); !ok || string(v) != "1" {
		t.Errorf("a = %q,%v; want \"1\",true", v, ok)
	}
	if v, ok := dbGet(t, db2, "b"); !ok || string(v) != "2" {
		t.Errorf("b = %q,%v; want \"2\",true", v, ok)
	}
}

func TestDBRebuildsFromObjwalWhenLocalMissing(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()

	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = db.Put(ctx, []byte("a"), []byte("1"))
	_ = db.Put(ctx, []byte("b"), []byte("2"))
	if err := db.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Fresh, empty local file but the SAME store: recovery must replay objwal.
	db2, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("reopen with empty local: %v", err)
	}
	defer db2.Close(ctx)

	if v, ok := dbGet(t, db2, "a"); !ok || string(v) != "1" {
		t.Errorf("a = %q,%v; want \"1\",true (objwal replay)", v, ok)
	}
	if v, ok := dbGet(t, db2, "b"); !ok || string(v) != "2" {
		t.Errorf("b = %q,%v; want \"2\",true (objwal replay)", v, ok)
	}
}

func TestDBConcurrentPutsDifferentKeys(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()
	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close(ctx)

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			errs[i] = db.Put(ctx, []byte(key), []byte(fmt.Sprintf("val-%d", i)))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("val-%d", i)
		if v, ok := dbGet(t, db, fmt.Sprintf("key-%d", i)); !ok || string(v) != want {
			t.Errorf("key-%d = %q,%v; want %q,true", i, v, ok, want)
		}
	}
}
