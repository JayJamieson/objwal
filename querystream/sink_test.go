package querystream

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

type kv struct {
	Seq uint64 `json:"seq"`
	Val string `json:"val"`
}

func decodeKV(rec wal.Record) (kv, error) {
	return kv{Seq: rec.Sequence, Val: string(rec.Data)}, nil
}

// produceWAL writes n single-record appends ("val-0".."val-{n-1}") to a fresh
// in-memory object store, rotating segments small so the manifest has several.
func produceWAL(t *testing.T, n int) (objectstore.ObjectStore, string) {
	t.Helper()
	ctx := context.Background()
	os := objectstore.NewInMemory()
	const manifest = "wal/manifest"
	p, err := wal.NewProducer(ctx, os, wal.ProducerConfig{
		ManifestPath:    manifest,
		SegmentPrefix:   "wal/seg",
		FlushInterval:   time.Hour,
		SegmentMaxBytes: 16, // force several segments
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
	return os, manifest
}

func itoa(i int) string { return strconv.Itoa(i) }

// readJSONL parses a part-*.jsonl file into (seq,val) pairs.
func readJSONL(t *testing.T, path string) []kv {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test helper reading files this test itself wrote
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var out []kv
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec struct {
			Seq uint64 `json:"seq"`
			Row kv     `json:"row"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("bad jsonl line %q: %v", sc.Text(), err)
		}
		// the wrapper seq must equal the row's own seq
		if rec.Seq != rec.Row.Seq {
			t.Fatalf("seq mismatch: wrapper %d row %d", rec.Seq, rec.Row.Seq)
		}
		out = append(out, rec.Row)
	}
	return out
}

func listParts(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "seq_bucket=*", "part-*-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(matches)
	return matches
}

func TestIngestPartitionsAndContents(t *testing.T) {
	ctx := context.Background()
	store, manifest := produceWAL(t, 25) // seqs 0..24
	dir := t.TempDir()

	s, err := NewSink[kv](Config{
		ObjectStore:  store,
		ManifestPath: manifest,
		Dir:          dir,
		BucketSize:   10, // buckets: 0=[0,10) 1=[10,20) 2=[20,30)
		MaxRows:      1_000_000,
		Cursor:       wal.NewFileCursorStore(filepath.Join(dir, "cursor")),
	}, decodeKV, JSONLEncoder[kv]{})
	if err != nil {
		t.Fatal(err)
	}

	n, err := s.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 25 {
		t.Fatalf("applied %d, want 25", n)
	}

	// Bucket-boundary splitting in a single poll => 3 files: 0-9, 10-19, 20-24.
	parts := listParts(t, dir)
	wantNames := []string{
		"seq_bucket=00000000000000000000/part-00000000000000000000-00000000000000000009.jsonl",
		"seq_bucket=00000000000000000001/part-00000000000000000010-00000000000000000019.jsonl",
		"seq_bucket=00000000000000000002/part-00000000000000000020-00000000000000000024.jsonl",
	}
	if len(parts) != 3 {
		t.Fatalf("got %d files, want 3: %v", len(parts), parts)
	}
	for i, p := range parts {
		if rel, _ := filepath.Rel(dir, p); rel != wantNames[i] {
			t.Fatalf("file %d = %s, want %s", i, rel, wantNames[i])
		}
	}

	// Contents: every seq 0..24 present once, in order, with matching values.
	var all []kv
	for _, p := range parts {
		all = append(all, readJSONL(t, p)...)
	}
	if len(all) != 25 {
		t.Fatalf("total rows %d, want 25", len(all))
	}
	for i, row := range all {
		if row.Seq != uint64(i) || row.Val != "val-"+itoa(i) {
			t.Fatalf("row %d = %+v, want seq %d val val-%d", i, row, i, i)
		}
	}

	// Watermark + catalog.
	high, ok := s.VisibleHigh()
	if !ok || high != 24 {
		t.Fatalf("VisibleHigh = %d,%v, want 24,true", high, ok)
	}
	if len(s.Catalog()) != 3 {
		t.Fatalf("catalog has %d files, want 3", len(s.Catalog()))
	}
}

func TestResumeAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store, manifest := produceWAL(t, 12) // seqs 0..11
	dir := t.TempDir()
	cursorPath := filepath.Join(dir, "cursor")
	mk := func() *Sink[kv] {
		s, err := NewSink[kv](Config{
			ObjectStore: store, ManifestPath: manifest, Dir: dir,
			BucketSize: 10, MaxRows: 1_000_000,
			Cursor: wal.NewFileCursorStore(cursorPath),
		}, decodeKV, JSONLEncoder[kv]{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	// First process: ingest everything.
	s1 := mk()
	if _, err := s1.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	filesAfterFirst := len(listParts(t, dir))

	// Second process (fresh sink, same dir + cursor): seeds visibleHigh from
	// disk and resumes from the persisted cursor. No new WAL data => no work.
	s2 := mk()
	if high, ok := s2.VisibleHigh(); !ok || high != 11 {
		t.Fatalf("resumed VisibleHigh = %d,%v, want 11,true", high, ok)
	}
	n, err := s2.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("resumed poll applied %d, want 0 (already ingested)", n)
	}
	if got := len(listParts(t, dir)); got != filesAfterFirst {
		t.Fatalf("resume created files: %d -> %d", filesAfterFirst, got)
	}

	// Append more to the WAL; the resumed sink picks up only the new records.
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath: manifest, SegmentPrefix: "wal/seg",
		FlushInterval: time.Hour, SegmentMaxBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 12; i < 18; i++ {
		if _, err := p.Append(ctx, [][]byte{[]byte("val-" + itoa(i))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	n, err = s2.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("applied %d new, want 6", n)
	}
	if high, ok := s2.VisibleHigh(); !ok || high != 17 {
		t.Fatalf("VisibleHigh after append = %d,%v, want 17,true", high, ok)
	}
	// seqs 12..17 are bucket 1 ([10,20)); a new file part-12-17 joins part-10-11.
	var all []kv
	for _, p := range listParts(t, dir) {
		all = append(all, readJSONL(t, p)...)
	}
	if len(all) != 18 {
		t.Fatalf("total rows %d, want 18", len(all))
	}
	for i, row := range all {
		if row.Seq != uint64(i) {
			t.Fatalf("row %d has seq %d", i, row.Seq)
		}
	}
}

// Re-running ingest over an already-covered range rewrites the same range-named
// files deterministically: same file set, identical contents, no duplicates.
func TestIdempotentReRun(t *testing.T) {
	ctx := context.Background()
	store, manifest := produceWAL(t, 15)
	dir := t.TempDir()

	run := func(startAt uint64) {
		s, err := NewSink[kv](Config{
			ObjectStore: store, ManifestPath: manifest, Dir: dir,
			BucketSize: 10, MaxRows: 1_000_000,
			StartAt: startAt, // no cursor: force a re-ingest from startAt
		}, decodeKV, JSONLEncoder[kv]{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	run(0)
	first := snapshotDir(t, dir)
	run(0) // identical re-run
	second := snapshotDir(t, dir)

	if len(first) != len(second) {
		t.Fatalf("re-run changed file count: %d -> %d", len(first), len(second))
	}
	for name, content := range first {
		if second[name] != content {
			t.Fatalf("re-run changed contents of %s", name)
		}
	}
}

// snapshotDir maps each part file's relative path to its contents.
func snapshotDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, p := range listParts(t, dir) {
		b, err := os.ReadFile(p) //nolint:gosec // test helper reading files this test itself wrote
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(dir, p)
		out[rel] = string(b)
	}
	return out
}
