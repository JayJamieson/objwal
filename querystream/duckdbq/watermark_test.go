package duckdbq

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func testWatermarkStore(t *testing.T, ws WatermarkStore) {
	t.Helper()
	ctx := context.Background()

	if v, ok, err := ws.Load(ctx); err != nil || ok || v != 0 {
		t.Fatalf("fresh Load = %d,%v,%v, want 0,false,nil", v, ok, err)
	}
	// First CAS from the zero/unset baseline.
	if err := ws.SaveCAS(ctx, 0, 5); err != nil {
		t.Fatalf("first SaveCAS: %v", err)
	}
	if v, ok, err := ws.Load(ctx); err != nil || !ok || v != 5 {
		t.Fatalf("Load after save = %d,%v,%v, want 5,true,nil", v, ok, err)
	}
	// Correct prev advances.
	if err := ws.SaveCAS(ctx, 5, 10); err != nil {
		t.Fatalf("advance SaveCAS: %v", err)
	}
	// Stale prev conflicts and does not mutate.
	if err := ws.SaveCAS(ctx, 5, 99); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale SaveCAS err = %v, want ErrConflict", err)
	}
	if v, _, _ := ws.Load(ctx); v != 10 {
		t.Fatalf("value after conflicted CAS = %d, want 10", v)
	}
}

func TestMemWatermarkStore(t *testing.T) {
	testWatermarkStore(t, NewMemWatermarkStore())
}

func TestFileWatermarkStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm")
	testWatermarkStore(t, NewFileWatermarkStore(path))

	// A fresh store on the same path resumes the persisted value.
	ws2 := NewFileWatermarkStore(path)
	if v, ok, err := ws2.Load(context.Background()); err != nil || !ok || v != 10 {
		t.Fatalf("resumed Load = %d,%v,%v, want 10,true,nil", v, ok, err)
	}
}
