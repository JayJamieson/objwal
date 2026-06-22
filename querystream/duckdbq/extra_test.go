package duckdbq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JayJamieson/objwal/querystream/parquetenc"
)

// writeTypedFile writes rows of an arbitrary row type to a single part file in
// the given bucket (used to forge overlaps and schema drift the helper-per-row
// writeFixtures can't).
func writeTypedFile[T any](t *testing.T, dir string, bucket, first, last uint64, rows []T) {
	t.Helper()
	enc := parquetenc.New[T](parquetenc.Options{Compression: "zstd"})
	bucketDir := filepath.Join(dir, fmt.Sprintf("seq_bucket=%020d", bucket))
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(bucketDir, fmt.Sprintf("part-%020d-%020d.parquet", first, last))
	if err := enc.Encode(path, nil, rows); err != nil {
		t.Fatal(err)
	}
}

// TestDedup: two files in the same bucket with overlapping seq ranges (the
// at-least-once partial-then-fuller case). Without Dedup the overlap is visible
// twice; with Dedup each seq appears exactly once.
func TestDedup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Overlap on seq 0,1,2.
	writeTypedFile(t, dir, 0, 0, 2, []kv{{0, "v0"}, {1, "v1"}, {2, "v2"}})
	writeTypedFile(t, dir, 0, 0, 4, []kv{{0, "v0"}, {1, "v1"}, {2, "v2"}, {3, "v3"}, {4, "v4"}})

	mk := func(dedup bool) *Engine {
		e, err := Open(Config{ReadGlob: localGlob(dir), BucketSize: 10, Dedup: dedup})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { e.Close() })
		return e
	}

	// Without dedup: 3 + 5 = 8 rows, seq 0 appears twice.
	rows, _, err := mk(false).Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanKV(t, rows); len(got) != 8 {
		t.Fatalf("no-dedup got %d rows, want 8", len(got))
	}

	// With dedup: 5 distinct seqs, each once, values intact.
	rows, _, err = mk(true).Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := scanKV(t, rows)
	if len(got) != 5 {
		t.Fatalf("dedup got %d rows, want 5", len(got))
	}
	for i, r := range got {
		if r.Seq != uint64(i) || r.Val != fmt.Sprintf("v%d", i) {
			t.Fatalf("dedup row %d = %+v", i, r)
		}
	}
}

// TestPruningFiresOnSeqBucket asserts (via EXPLAIN ANALYZE through the real
// bound view) that a tight seq window scans a single partition file, not all of
// them — the seq_bucket lower bound the query layer injects is what enables it.
func TestPruningFiresOnSeqBucket(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seqs := make([]uint64, 100) // 10 buckets x 10 seqs => 10 files
	for i := range seqs {
		seqs[i] = uint64(i)
	}
	writeFixtures(t, dir, 10, seqs)
	e := openLocal(t, dir, 10)

	// Build the same bound view Query would for Cumulative Start=95 (bucketLo=9),
	// then EXPLAIN ANALYZE the user query against it.
	hi, _, err := e.VisibleHigh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loExcl, hasLo, bucketLo := resolveWindow(Query{Mode: Cumulative, Start: 95}, 0, e.cfg.BucketSize)
	if _, err := e.db.ExecContext(ctx, e.buildViewSQL(loExcl, hasLo, hi, bucketLo)); err != nil {
		t.Fatal(err)
	}
	plan := explainAnalyze(t, ctx, e.db, "SELECT * FROM records")
	if !strings.Contains(plan, "Total Files Read: 1") {
		t.Fatalf("expected single-file scan, plan was:\n%s", plan)
	}
	if strings.Contains(plan, "Total Files Read: 10") {
		t.Fatalf("expected pruning, but all 10 files were read:\n%s", plan)
	}

	// Control: a full scan (Start=0, no bucket bound) reads every file.
	if _, err := e.db.ExecContext(ctx, e.buildViewSQL(0, false, hi, 0)); err != nil {
		t.Fatal(err)
	}
	full := explainAnalyze(t, ctx, e.db, "SELECT * FROM records")
	if !strings.Contains(full, "Total Files Read: 10") {
		t.Fatalf("expected full scan of 10 files, plan was:\n%s", full)
	}
}

func explainAnalyze(t *testing.T, ctx context.Context, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		sb.WriteString(v)
		sb.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

type kvExtra struct {
	Seq   uint64 `parquet:"seq"`
	Val   string `parquet:"val"`
	Extra string `parquet:"extra"`
}

// TestSchemaEvolution: a later file adds a column. union_by_name keeps the older
// files queryable, with the new column NULL for rows that predate it.
func TestSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Old schema (no "extra") in bucket 0; new schema in bucket 1.
	writeTypedFile(t, dir, 0, 0, 1, []kv{{0, "v0"}, {1, "v1"}})
	writeTypedFile(t, dir, 1, 10, 11, []kvExtra{{10, "v10", "x10"}, {11, "v11", "x11"}})

	e := openLocal(t, dir, 10)
	rows, _, err := e.Query(ctx, Query{
		SQL:  "SELECT seq, val, extra FROM records ORDER BY seq",
		Mode: Cumulative,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type out struct {
		seq   uint64
		val   string
		extra sql.NullString
	}
	var got []out
	for rows.Next() {
		var o out
		if err := rows.Scan(&o.seq, &o.val, &o.extra); err != nil {
			t.Fatal(err)
		}
		got = append(got, o)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d rows, want 4", len(got))
	}
	// Old rows: extra is NULL. New rows: extra populated.
	for _, o := range got {
		switch o.seq {
		case 0, 1:
			if o.extra.Valid {
				t.Fatalf("seq %d extra=%q, want NULL (old schema)", o.seq, o.extra.String)
			}
		case 10, 11:
			if !o.extra.Valid || o.extra.String != fmt.Sprintf("x%d", o.seq) {
				t.Fatalf("seq %d extra=%v, want x%d", o.seq, o.extra, o.seq)
			}
		}
	}
}
