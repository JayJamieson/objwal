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

// appendWAL appends "val-{lo}".."val-{hi-1}" to an existing WAL, so a sink can
// observe new records arriving after it has already ingested an earlier range.
func appendWAL(t *testing.T, store objectstore.ObjectStore, manifest string, lo, hi int) {
	t.Helper()
	ctx := context.Background()
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:    manifest,
		SegmentPrefix:   "wal/seg",
		FlushInterval:   time.Hour,
		SegmentMaxBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := lo; i < hi; i++ {
		if _, err := p.Append(ctx, [][]byte{[]byte("val-" + itoa(i))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestEndToEnd is the SPEC §7 smoke test: produce a WAL, materialize it to
// Parquet with the Sink + Parquet encoder, then query it through duckdbq -
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

	// Incremental via a watermark loop: first pass reads from next-to-read wm=21
	// (the tail 21..24). The watermark advances to high+1 after consuming.
	wm := NewMemWatermarkStore()
	prev, _, _ := wm.Load(ctx)
	rows, newHigh, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, max64(prev, 21))
	if err != nil {
		t.Fatal(err)
	}
	tail := scanKV(t, rows)
	if newHigh != 24 || len(tail) != 4 || tail[0].Seq != 21 || tail[3].Seq != 24 {
		t.Fatalf("incremental tail high=%d rows=%d [%d..], want 24,4,21", newHigh, len(tail), seqOf(tail))
	}
	if err := wm.SaveCAS(ctx, prev, newHigh+1); err != nil {
		t.Fatalf("persist watermark: %v", err)
	}

	// Second pass from the advanced watermark (25): nothing new.
	prev, _, _ = wm.Load(ctx)
	rows, newHigh2, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, prev)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanKV(t, rows); len(got) != 0 || newHigh2 != 24 {
		t.Fatalf("second incremental pass rows=%d high=%d, want 0,24", len(got), newHigh2)
	}

	// Pruning: the incremental window from wm=21 scans only seq_bucket=2 (1 of 3).
	loExcl, hasLo, bucketLo := resolveWindow(Query{Mode: Incremental}, 21, e.cfg.BucketSize)
	if _, err := e.db.ExecContext(ctx, e.buildViewSQL(loExcl, hasLo, hi, bucketLo)); err != nil {
		t.Fatal(err)
	}
	plan := explainAnalyze(t, ctx, e.db, "SELECT * FROM records")
	if !strings.Contains(plan, "Total Files Read: 1") || strings.Contains(plan, "Total Files Read: 3") {
		t.Fatalf("expected pruning to a single file, plan:\n%s", plan)
	}
}

// TestInProcessLoop wires the full continuous loop: a Sink ingests a WAL into
// Parquet and, on every watermark advance, signals its coalescing Notify
// channel; a duckdbq.Stream consumes that channel, re-queries the engine, and
// emits the new tail. No external scheduler - the Sink's own flush drives the
// Stream. Driven in two ingest stages to prove it reacts to fresh data more
// than once.
func TestInProcessLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, manifest := produceWAL(t, 5) // seqs 0..4
	dir := t.TempDir()

	sink, err := querystream.NewSink[kv](querystream.Config{
		ObjectStore:  store,
		ManifestPath: manifest,
		Dir:          dir,
		BucketSize:   100, // one bucket: each Poll finalizes a single file
		MaxRows:      1_000_000,
		Cursor:       wal.NewFileCursorStore(filepath.Join(dir, "cursor")),
	}, decodeKV, parquetenc.New[kv](parquetenc.Options{Compression: "zstd"}))
	if err != nil {
		t.Fatal(err)
	}

	e, err := Open(Config{ReadGlob: localGlob(dir), BucketSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	wm := NewMemWatermarkStore()
	out, errc, err := Stream[srow](ctx, e, StreamConfig[srow]{
		Query:     Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental},
		Notify:    sink.Notify(), // the loop: sink flush -> stream re-query
		Watermark: wm,
		Bind:      bindSrow,
		Buffer:    4,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stage 1: ingest seqs 0..4. The flush advances visibleHigh to 4 and signals
	// Notify; the stream wakes and, from wm=0 (nothing read), emits the full set
	// 0..4 - the 0-based first record is included, not skipped.
	if n, err := sink.Poll(ctx); err != nil || n != 5 {
		t.Fatalf("stage1 Poll = %d,%v, want 5,nil", n, err)
	}
	r1 := <-out
	if r1.High != 4 || len(r1.Rows) != 5 || r1.Rows[0].Seq != 0 || r1.Rows[4].Seq != 4 {
		t.Fatalf("stage1 emit = high %d rows %d [%d..], want high 4 rows 5 from 0", r1.High, len(r1.Rows), seqOfSrow(r1.Rows))
	}
	r1.Recycle()

	// Stage 2: append seqs 5..9 to the WAL; the next Poll finalizes a new file,
	// signals Notify again, and the stream emits only the new tail 5..9.
	appendWAL(t, store, manifest, 5, 10)
	if n, err := sink.Poll(ctx); err != nil || n != 5 {
		t.Fatalf("stage2 Poll = %d,%v, want 5,nil", n, err)
	}
	r2 := <-out
	if r2.High != 9 || len(r2.Rows) != 5 || r2.Rows[0].Seq != 5 || r2.Rows[4].Seq != 9 {
		t.Fatalf("stage2 emit = high %d rows %d [%d..], want high 9 rows 5 from 5", r2.High, len(r2.Rows), seqOfSrow(r2.Rows))
	}
	r2.Recycle()

	// Watermark is next-to-read: after delivering through 9 it sits at 10.
	if v, _, _ := wm.Load(ctx); v != 10 {
		t.Fatalf("watermark after loop = %d, want 10", v)
	}

	cancel()
	if err := <-errc; err != nil && err != context.Canceled {
		t.Fatalf("stream error: %v", err)
	}
}

func seqOfSrow(rows []srow) uint64 {
	if len(rows) == 0 {
		return 0
	}
	return rows[0].Seq
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
