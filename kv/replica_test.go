package kv

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

func replicaConfig(dir string) ReplicaConfig {
	return ReplicaConfig{
		ManifestPath: "wal/manifest",
		LocalPath:    filepath.Join(dir, "replica-data"),
		PollInterval: 5 * time.Millisecond,
	}
}

func repGet(t *testing.T, r *Replica, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("replica Get %q: %v", key, err)
	}
	return v, ok
}

func TestReplicaTailsPrimaryWrites(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()

	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open primary: %v", err)
	}
	defer db.Close(ctx)
	_ = db.Put(ctx, []byte("a"), []byte("1"))
	_ = db.Put(ctx, []byte("b"), []byte("2"))

	r, err := OpenReplica(ctx, store, replicaConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReplica: %v", err)
	}
	defer r.Close()

	if _, err := r.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if v, ok := repGet(t, r, "a"); !ok || string(v) != "1" {
		t.Errorf("a = %q,%v; want \"1\",true", v, ok)
	}
	if v, ok := repGet(t, r, "b"); !ok || string(v) != "2" {
		t.Errorf("b = %q,%v; want \"2\",true", v, ok)
	}
}

func TestReplicaSeesDelete(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewInMemory()

	db, err := Open(ctx, store, testConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("Open primary: %v", err)
	}
	defer db.Close(ctx)
	_ = db.Put(ctx, []byte("k"), []byte("v"))
	_ = db.Delete(ctx, []byte("k"))

	r, err := OpenReplica(ctx, store, replicaConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReplica: %v", err)
	}
	defer r.Close()

	if _, err := r.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, ok := repGet(t, r, "k"); ok {
		t.Error("replica should see the delete as a miss")
	}
}
