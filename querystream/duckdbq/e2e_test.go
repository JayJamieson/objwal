package duckdbq

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/querystream"
	"github.com/JayJamieson/objwal/querystream/parquetenc"
	"github.com/JayJamieson/objwal/wal"
)

func decodeKV(rec wal.Record) (kv, error) {
	return kv{Seq: rec.Sequence, Val: string(rec.Data)}, nil
}

// produceWAL writes n single-record appends "val-0".."val-{n-1}" to a fresh
// in-memory object store, rotating segments small so the manifest spans several.
func produceWAL(t *testing.T, n int) (objectstore.ObjectStore, string) {
	t.Helper()
	ctx := context.Background()
	store := objectstore.NewInMemory()
	const manifest = "wal/manifest"
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:    manifest,
		SegmentPrefix:   "wal/seg",
		FlushInterval:   time.Hour,
		SegmentMaxBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := p.Append(ctx, [][]byte{[]byte("val-" + itoa(i))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return store, manifest
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }

// TestEndToEnd is the SPEC §7 smoke test: produce a WAL, materialize it to
// Parquet with the Sink + Parquet encoder, then query it through duckdbq —
// Cumulative returns every row in seq order, Incremental returns only the tail
// after a watermark (driven by a real WatermarkStore loop), and a tight window
// prunes to a single partition file.
func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	store, manifest := produceWAL(t, 25) // seqs 0..24
	dir := t.TempDir()

	// Ingest: WAL -> Parquet (buckets of 10 => seq_bucket 0,1,2).
	sink, err := querystream.NewSink[kv](querystream.Config{
		ObjectStore:  store,
		ManifestPath: manifest,
		Dir:          dir,
		BucketSize:   10,
		MaxRows:      1_000_000,
		Cursor:       wal.NewFileCursorStore(filepath.Join(dir, "cursor")),
	}, decodeKV, parquetenc.New[kv](parquetenc.Options{Compression: "zstd"}))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := sink.Poll(ctx); err != nil || n != 25 {
		t.Fatalf("sink.Poll = %d,%v, want 25,nil", n, err)
	}

	e, err := Open(Config{ReadGlob: localGlob(dir), BucketSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// VisibleHigh agrees with the sink.
	hi, ok, err := e.VisibleHigh(ctx)
	if err != nil || !ok || hi != 24 {
		t.Fatalf("VisibleHigh = %d,%v,%v, want 24,true,nil", hi, ok, err)
	}
	if sh, _ := sink.VisibleHigh(); sh != hi {
		t.Fatalf("sink VisibleHigh %d != engine %d", sh, hi)
	}

	// Cumulative: every row, in seq order, values intact.
	rows, high, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative}, 0)
	if err != nil {
		t.Fatal(err)
	}
	all := scanKV(t, rows)
	if high != 24 || len(all) != 25 {
		t.Fatalf("cumulative high=%d rows=%d, want 24,25", high, len(all))
	}
	for i, r := range all {
		if r.Seq != uint64(i) || r.Val != "val-"+itoa(i) {
			t.Fatalf("row %d = %+v, want seq %d val val-%d", i, r, i, i)
		}
	}

	// Incremental via a watermark loop: first pass returns the tail past wm=20.
	wm := NewMemWatermarkStore()
	prev, _, _ := wm.Load(ctx)
	rows, newHigh, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, max64(prev, 20))
	if err != nil {
		t.Fatal(err)
	}
	tail := scanKV(t, rows)
	if newHigh != 24 || len(tail) != 4 || tail[0].Seq != 21 || tail[3].Seq != 24 {
		t.Fatalf("incremental tail high=%d rows=%d [%d..], want 24,4,21", newHigh, len(tail), seqOf(tail))
	}
	if err := wm.SaveCAS(ctx, prev, newHigh); err != nil {
		t.Fatalf("persist watermark: %v", err)
	}

	// Second pass from the advanced watermark: nothing new.
	prev, _, _ = wm.Load(ctx)
	rows, newHigh2, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, prev)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanKV(t, rows); len(got) != 0 || newHigh2 != 24 {
		t.Fatalf("second incremental pass rows=%d high=%d, want 0,24", len(got), newHigh2)
	}

	// Pruning: the incremental window past wm=20 scans only seq_bucket=2 (1 of 3).
	loExcl, hasLo, bucketLo := resolveWindow(Query{Mode: Incremental}, 20, e.cfg.BucketSize)
	if _, err := e.db.ExecContext(ctx, e.buildViewSQL(loExcl, hasLo, hi, bucketLo)); err != nil {
		t.Fatal(err)
	}
	plan := explainAnalyze(t, ctx, e.db, "SELECT * FROM records")
	if !strings.Contains(plan, "Total Files Read: 1") || strings.Contains(plan, "Total Files Read: 3") {
		t.Fatalf("expected pruning to a single file, plan:\n%s", plan)
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func seqOf(rows []kv) uint64 {
	if len(rows) == 0 {
		return 0
	}
	return rows[0].Seq
}
