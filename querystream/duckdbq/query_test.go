package duckdbq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JayJamieson/objwal/querystream/parquetenc"
)

type kv struct {
	Seq uint64 `parquet:"seq"`
	Val string `parquet:"val"`
}

// writeFixtures writes seqs (must be ascending) into the sink's hive layout
// <dir>/seq_bucket=<b>/part-<first>-<last>.parquet, one file per bucket, using
// the real Parquet encoder.
func writeFixtures(t *testing.T, dir string, bucketSize uint64, seqs []uint64) {
	t.Helper()
	enc := parquetenc.New[kv](parquetenc.Options{Compression: "zstd"})
	i := 0
	for i < len(seqs) {
		b := seqs[i] / bucketSize
		j := i
		for j < len(seqs) && seqs[j]/bucketSize == b {
			j++
		}
		run := seqs[i:j]
		rows := make([]kv, len(run))
		for k, s := range run {
			rows[k] = kv{Seq: s, Val: fmt.Sprintf("v%d", s)}
		}
		bucketDir := filepath.Join(dir, fmt.Sprintf("seq_bucket=%020d", b))
		if err := os.MkdirAll(bucketDir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(bucketDir, fmt.Sprintf("part-%020d-%020d.parquet", run[0], run[len(run)-1]))
		if err := enc.Encode(path, run, rows); err != nil {
			t.Fatal(err)
		}
		i = j
	}
}

func localGlob(dir string) string {
	return filepath.Join(dir, "seq_bucket=*", "*.parquet")
}

func scanKV(t *testing.T, rows *sql.Rows) []kv {
	t.Helper()
	defer rows.Close()
	var out []kv
	for rows.Next() {
		var r kv
		if err := rows.Scan(&r.Seq, &r.Val); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func openLocal(t *testing.T, dir string, bucketSize uint64) *Engine {
	t.Helper()
	e, err := Open(Config{
		ReadGlob:   localGlob(dir),
		BucketSize: bucketSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestVisibleHighLocal(t *testing.T) {
	dir := t.TempDir()
	writeFixtures(t, dir, 10, []uint64{0, 1, 2, 10, 11, 20, 21, 22, 23})
	e := openLocal(t, dir, 10)

	hi, ok, err := e.VisibleHigh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || hi != 23 {
		t.Fatalf("VisibleHigh = %d,%v, want 23,true", hi, ok)
	}
}

func TestVisibleHighEmpty(t *testing.T) {
	dir := t.TempDir()
	e := openLocal(t, dir, 10)
	_, ok, err := e.VisibleHigh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("VisibleHigh ok=true over empty dir, want false")
	}
}

func TestCumulative(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFixtures(t, dir, 10, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	e := openLocal(t, dir, 10)

	// Start=0 returns everything in seq order.
	rows, high, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative, Start: 0}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := scanKV(t, rows)
	if high != 12 {
		t.Fatalf("newHigh = %d, want 12", high)
	}
	if len(got) != 13 {
		t.Fatalf("got %d rows, want 13", len(got))
	}
	for i, r := range got {
		if r.Seq != uint64(i) || r.Val != fmt.Sprintf("v%d", i) {
			t.Fatalf("row %d = %+v", i, r)
		}
	}

	// Start=5 returns seq>=5.
	rows, _, err = e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative, Start: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got = scanKV(t, rows)
	if len(got) != 8 || got[0].Seq != 5 {
		t.Fatalf("Start=5 got %d rows, first seq %d, want 8 rows starting at 5", len(got), got[0].Seq)
	}
}

func TestIncremental(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFixtures(t, dir, 10, []uint64{0, 1, 2, 3, 4, 5, 6, 7})
	e := openLocal(t, dir, 10)

	// First pass from wm=0 ("nothing read yet"): no lower bound, so the full set
	// 0..7 is returned including the 0-based first record.
	rows, high, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := scanKV(t, rows)
	if high != 7 {
		t.Fatalf("newHigh = %d, want 7", high)
	}
	if len(got) != 8 || got[0].Seq != 0 || got[len(got)-1].Seq != 7 {
		t.Fatalf("incremental from 0 got %d rows [%d..%d], want 8 [0..7]", len(got), got[0].Seq, got[len(got)-1].Seq)
	}

	// Resume from the next-to-read watermark (high+1): no new data => zero rows.
	rows, high2, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, high+1)
	if err != nil {
		t.Fatal(err)
	}
	got = scanKV(t, rows)
	if len(got) != 0 {
		t.Fatalf("second incremental pass got %d rows, want 0", len(got))
	}
	if high2 != 7 {
		t.Fatalf("newHigh stayed %d, want 7", high2)
	}

	// Append more data, then resume from high2+1 (= 8) returns only the tail.
	writeFixtures(t, dir, 10, []uint64{8, 9, 10, 11})
	rows, high3, err := e.Query(ctx, Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental}, high2+1)
	if err != nil {
		t.Fatal(err)
	}
	got = scanKV(t, rows)
	if high3 != 11 {
		t.Fatalf("newHigh = %d, want 11", high3)
	}
	if len(got) != 4 || got[0].Seq != 8 || got[3].Seq != 11 {
		t.Fatalf("incremental tail got %d rows [%d..%d]", len(got), got[0].Seq, got[len(got)-1].Seq)
	}
}

func TestQueryNoData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openLocal(t, dir, 10)
	_, _, err := e.Query(ctx, Query{SQL: "SELECT seq FROM records", Mode: Cumulative}, 0)
	if err != ErrNoData {
		t.Fatalf("Query over empty dataset err = %v, want ErrNoData", err)
	}
}
