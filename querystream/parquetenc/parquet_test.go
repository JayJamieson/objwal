package parquetenc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type row struct {
	Seq uint64 `parquet:"seq"`
	Key string `parquet:"key"`
	Val string `parquet:"val"`
}

// chunkBounds returns the min/max seq across every page of a column chunk's
// column index (a chunk may hold more than one page).
func chunkBounds(t *testing.T, c parquet.ColumnChunk) (lo, hi uint64) {
	t.Helper()
	ci, err := c.ColumnIndex()
	if err != nil {
		t.Fatalf("column index: %v", err)
	}
	for p := 0; p < ci.NumPages(); p++ {
		mn, mx := ci.MinValue(p).Uint64(), ci.MaxValue(p).Uint64()
		if p == 0 || mn < lo {
			lo = mn
		}
		if p == 0 || mx > hi {
			hi = mx
		}
	}
	return lo, hi
}

// seqLeafIndex finds the leaf-column position of the "seq" column.
func seqLeafIndex(t *testing.T, f *parquet.File) int {
	t.Helper()
	for i, col := range f.Schema().Columns() {
		// flat schema: one path element per leaf column
		if len(col) == 1 && col[0] == "seq" {
			return i
		}
	}
	t.Fatalf("seq column not found in schema %v", f.Schema().Columns())
	return -1
}

// TestEncodeRoundTrip writes a seq-sorted batch with the Parquet encoder and
// reads it back, asserting: the schema carries the seq/key/val columns, rows
// come back in ascending seq order with intact values, and the per-row-group
// column index carries seq min/max statistics (the pruning workhorse) that tile
// the sequence range without overlap.
func TestEncodeRoundTrip(t *testing.T) {
	seqs := []uint64{10, 11, 12, 13, 14}
	rows := make([]row, len(seqs))
	for i, s := range seqs {
		rows[i] = row{Seq: s, Key: "k" + itoa(int(s)), Val: "v" + itoa(int(s))}
	}

	// Small row groups so stats are exercised across multiple groups.
	enc := New[row](Options{Compression: "zstd", RowGroupTargetRows: 2})
	if enc.Ext() != ".parquet" {
		t.Fatalf("Ext = %q, want .parquet", enc.Ext())
	}

	path := filepath.Join(t.TempDir(), "part.parquet")
	if err := enc.Encode(path, seqs, rows); err != nil {
		t.Fatalf("encode: %v", err)
	}

	fi, err := os.Open(path) //nolint:gosec // test helper reading a file this test itself wrote
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fi.Close() }()
	st, _ := fi.Stat()
	f, err := parquet.OpenFile(fi, st.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	// Schema columns present.
	want := map[string]bool{"seq": false, "key": false, "val": false}
	for _, col := range f.Schema().Columns() {
		if len(col) == 1 {
			if _, ok := want[col[0]]; ok {
				want[col[0]] = true
			}
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("schema missing column %q (have %v)", name, f.Schema().Columns())
		}
	}

	// Read rows back; assert ascending seq and intact values.
	r := parquet.NewGenericReader[row](f)
	defer func() { _ = r.Close() }()
	got := make([]row, f.NumRows())
	n, err := r.Read(got)
	if err != nil && n != len(got) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	got = got[:n]
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	for i := range got {
		if got[i] != rows[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], rows[i])
		}
		if i > 0 && got[i].Seq <= got[i-1].Seq {
			t.Fatalf("seq not ascending at %d: %d after %d", i, got[i].Seq, got[i-1].Seq)
		}
	}

	// Row-group stats: seq column index min/max must tile [10,14] ascending,
	// non-overlapping. With RowGroupTargetRows=2 we expect >1 group.
	rgs := f.RowGroups()
	if len(rgs) < 2 {
		t.Fatalf("got %d row groups, want >=2 (RowGroupTargetRows=2)", len(rgs))
	}
	col := seqLeafIndex(t, f)
	var prevHi uint64
	var overallLo, overallHi uint64
	for i, rg := range rgs {
		lo, hi := chunkBounds(t, rg.ColumnChunks()[col])
		if lo > hi {
			t.Fatalf("rg %d: lo %d > hi %d", i, lo, hi)
		}
		if i == 0 {
			overallLo = lo
		} else if lo <= prevHi {
			t.Fatalf("rg %d min %d overlaps prev max %d", i, lo, prevHi)
		}
		prevHi = hi
		overallHi = hi
	}
	if overallLo != 10 || overallHi != 14 {
		t.Fatalf("overall seq stats = [%d,%d], want [10,14]", overallLo, overallHi)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
