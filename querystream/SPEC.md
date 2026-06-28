# querystream — design & build spec (Phase 1 done, Phase 2 to build)

Handoff for a fresh Claude instance with no network restrictions. This specs the
Parquet/Arrow + DuckDB query layer that sits on top of the objwal replication
WAL (`github.com/JayJamieson/objwal`). **Phase 1 (ingest) is implemented and
tested; Phase 2 (DuckDB query) is fully designed here and is what you build
next.** Treat the "Settled decisions" as fixed — they were converged on over a
long design session; don't relitigate them.

---

## 0. Environment & where things live

- Module: `github.com/JayJamieson/objwal`, working dir `go-buffer/`. Sandbox Go
  was **1.22.2**; deps are **vendored** (`GOFLAGS=-mod=vendor`). Your local Go
  should be **>= 1.24** (parquet-go v0.30 needs it).
- Build/test core: `GOFLAGS=-mod=vendor go test -race -count=1 ./...` (runs the
  WAL + the querystream package; the nested parquetenc/duckdbq modules are
  separate and excluded from `./...`).
- Code:
  - `wal/` — the replication WAL (manifest, producer, replica, cursor, applier).
  - `querystream/` — **Phase 1, DONE**: ingest sink (this package).
  - `querystream/parquetenc/` — **nested module**, real Parquet encoder. Written,
    **not compiled in the sandbox** (Go 1.24 + parquet-go unfetchable there).
    Build & test it first in your env.
  - `querystream/duckdbq/` — **Phase 2, TO BUILD**: DuckDB query layer (does not
    exist yet).
- `wal/ARCHITECTURE.md` and `CLAUDE.md` carry the WAL's own design + status.

### Why querystream core lives *inside* the buffer module
The sandbox couldn't fetch a separate module's deps, so the core was placed at
`buffer/querystream` to reuse the existing vendor tree. It only imports
`wal` + `objectstore` + stdlib, so it adds **no** new requires to the buffer
module. The heavy/CGO deps (parquet-go, DuckDB) are isolated in **nested
modules** with their own `go.mod`, which is what preserves the dependency
isolation we actually care about. **You may promote `querystream` to its own
top-level module** (move the dir, add `go.mod` with `replace
github.com/JayJamieson/objwal => ../go-buffer`, `go mod tidy`); nothing in the
code prevents it.

---

## 1. What this module is

A **consumer** that tails the WAL and materializes it into seq-partitioned,
seq-sorted column files that an external SQL engine (DuckDB) queries. Two halves:

- **Write/ingest half (Phase 1, done):** `wal.Replica` → `Applier` → typed
  `Decoder` → batch → finalized Parquet files named by the record-sequence range
  they cover. Pure Go (parquet-go), no CGO.
- **Read/query half (Phase 2, to build):** DuckDB over `read_parquet(glob)`,
  with bounded views per query. CGO. Stateless; re-globs each query so new files
  appear automatically; works over a local path or `s3://` (httpfs) unchanged.

Parquet files are the durable handoff between the halves. Arrow is **optional**
and only relevant later as a fast in-memory path (Arrow C-Data → DuckDB) to skip
the file round-trip for the freshest rows.

---

## 2. Settled decisions (do not relitigate)

1. **Separate module intent; nested isolation for parquet/duckdb (CGO).** WAL
   consumers must never pull parquet/duckdb into their graph.
2. **parquet-go writer + DuckDB reader-over-files split.** Writer controls
   layout/sort/stats; DuckDB is a stateless reader pointed at the files.
3. **Decoder seam.** `Decoder[T] func(wal.Record) (T, error)` — the supplier
   function. It's a typed interface in *this* module, adapted internally to the
   core `wal.Applier`, so the plain `Applier` signature never changes.
4. **Sequence is the default PK** and also the dedup key. The sink always knows
   `rec.Sequence` for partitioning; the row type `T` should carry it in a `seq`
   column so it lands in the file for querying/pruning/dedup.
5. **Partitioning = sequence-range buckets, files seq-sorted, `seq` (+ optional
   `ts`) columns.** Directory `seq_bucket=<n>/part-<first>-<last>.parquet`,
   `bucket = firstSeq / BucketSize`. Rationale: every access pattern (start from
   seq/segment, incremental since-watermark, replay-from-X) is a seq-range scan;
   data arrives already sorted so **row-group min/max stats on `seq` give
   automatic pruning for `WHERE seq >= X` with zero extra work** — this is the
   workhorse. Time queries prune acceptably via `ts` row-group stats because seq
   and ts are monotonically correlated, so **no time partition is needed**.
   DuckDB will *not* derive `seq_bucket` from a `seq` predicate, so for file-
   level partition pruning the query layer injects a `seq_bucket` bound itself.
6. **Three watermarks** (the correctness crux for continuous queries):
   - `ingestCursor` — next WAL sequence to read; durable (`wal.CursorStore`).
     Advances **only after** rows are durable in a finalized file. Doubles as the
     retention low-watermark for WAL GC.
   - `visibleHigh` — highest seq in a finalized, queryable file. Lags
     ingestCursor (buffered-but-unflushed rows aren't visible). Queries read
     `seq <= visibleHigh` so they never catch a half-written file.
   - `queryWatermark` — per continuous-query stream (Incremental mode only); how
     far that stream has returned. Durable if the query runs out-of-process.
7. **Two query modes:**
   - **Incremental** ("new only"): window `queryWatermark < seq <= visibleHigh`;
     advance the watermark after a successful emit.
   - **Cumulative** ("new + previously loaded"): window `Start <= seq <=
     visibleHigh`; re-scan each trigger.
8. **Bounded-view binding.** The query layer never parses user SQL. It defines a
   base view `read_parquet(glob, hive_partitioning=true, union_by_name=true)`,
   then a per-query `records` view = `SELECT * FROM base WHERE seq > <lo> AND seq
   <= <hi>` (lo/hi inlined, module-controlled). User SQL selects `FROM records`.
9. **Local FS writes; caller persists to S3.** DuckDB reads wherever the files
   are; pointing the read glob at `s3://…` gives S3 reads "for free" modulo
   `INSTALL/LOAD httpfs` + a credential secret.
10. **At-least-once ingest → exactly-once on read via seq dedup.** The WAL is
    at-least-once; a retry after a mid-segment failure can produce overlapping
    seq ranges across files. The query layer dedups by `seq` (the PK), making
    reads exactly-once. Range-named files mean clean re-runs overwrite
    identically; only the partial-then-fuller case creates overlap, which dedup
    handles.

---

## 3. objwal / WAL facts you need (the substrate)

- **`wal.Record{ Sequence uint64; GroupMeta []byte; Data []byte }`.** `Sequence`
  is **per-record** (global, monotonic, gap-free). NOTE: the doc comment on the
  struct still says "per-segment" — that's **stale**; per-record sequencing
  shipped (footer v3). `Data` is opaque framed bytes; `GroupMeta` is the
  per-`Append` metadata.
- **Per-record sequencing (manifest footer v3).** Each manifest `Entry` has
  `Sequence` and `Count`; it owns the half-open record range
  `[Sequence, Sequence+Count)`; record *i* has sequence `Sequence+i`. Consumer
  APIs on `*wal.Manifest`:
  - `Locate(seq) (Entry, offsetInSegment int, found bool)` — which segment holds
    `seq` and how many leading records to skip.
  - `EntriesContaining(seq) ([]Entry, error)` — the contiguous suffix of segments
    to replay from `seq` onward.
  - Legacy v1/v2 entries read back with `Count == 0` (one slot, whole-segment).
- **`wal.Applier` interface:** `Apply(ctx, rec Record) error`. `wal.ApplyFunc`
  adapts a func. `wal.TypedApplier(decode, handle)` is a typed convenience.
  Apply MUST be idempotent (at-least-once).
- **`wal.Replica`:** `NewReplica(os objectstore.ObjectStore, apply Applier, cfg
  ReplicaConfig) *Replica`. `ReplicaConfig{ ManifestPath string; StartAt uint64;
  Cursor CursorStore; CursorSaveInterval int }`. `Poll(ctx) (int, error)` applies
  all newly-committed records from the in-memory cursor; the cursor (`r.next`)
  persists across Poll calls within a process. **Mid-segment resume** works:
  `StartAt` can be any record sequence, including inside a segment.
- **`wal.CursorStore`:** `Load(ctx) (uint64, bool, error)`, `Save(ctx, next
  uint64) error`. Implementations: `MemCursorStore`, `FileCursorStore`
  (`wal.NewFileCursorStore(path)`).
- **`wal.Store`:** `wal.NewStore(os, path)`; `Load(ctx) (*Manifest, *UpdateVersion,
  bool, error)`. `*Manifest.Entries() ([]Entry, error)` (sequence order).
- **Producer `Durability`:** `Wait(ctx) (uint64, error)` → the append's first
  record sequence; `WaitRange(ctx) (SeqRange, error)` where
  `SeqRange{First uint64; Count int}` with `End()/Last()/At(i)/Contains/All()`;
  `Count() int`. A batched `Append(ctx, records [][]byte, meta []byte)` writes
  multiple records and returns ONE base sequence; the records occupy
  `[First, First+Count)`.
- **`objectstore.ObjectStore`:** `Get/Put/PutOpts{PutCreate→If-None-Match,
  PutUpdate→If-Match, PutOverwrite}/List/Delete`; sentinels
  `ErrNotFound/ErrPreconditionFailed/ErrAlreadyExists`; `objectstore.NewInMemory()`.
  The **conditional writes** (If-Match/If-None-Match) are what you can build a
  CAS-backed watermark store on for out-of-process Incremental queries.
  CAUTION: the `objectstore` package also contains the S3 adapter (imports
  aws-sdk-go-v2); importing `objectstore` pulls aws into the build graph. In the
  sandbox that's fine (vendored). If you split querystream into its own module,
  either keep objectstore's vendor available or move the S3 adapter to an
  `objectstore/s3` subpackage so core stays stdlib-only.

---

## 4. Phase 1 — ingest sink (IMPLEMENTED & TESTED)

Files: `querystream/types.go`, `querystream/sink.go`, `querystream/sink_test.go`
(3 tests, `-race`-clean). `querystream/parquetenc/` (nested module; build in your env).

### API surface (as built)

```go
type Decoder[T any] func(rec wal.Record) (T, error)

type Encoder[T any] interface {
    Encode(path string, seqs []uint64, rows []T) error // write seq-sorted batch to path
    Ext() string                                       // ".parquet", ".jsonl", …
}

type FileInfo struct { Path string; Bucket, FirstSeq, LastSeq uint64; Rows int }

type Config struct {
    ObjectStore  objectstore.ObjectStore
    ManifestPath string
    StartAt      uint64          // first seq if no cursor persisted
    Cursor       wal.CursorStore // persisted value overrides StartAt
    Dir          string          // local output root
    BucketSize   uint64          // seqs per bucket (default 1<<20)
    MaxRows      int             // rows/file (default 50_000)
    MaxBytes     int             // approx source-bytes/file (0 = off)
    PollInterval time.Duration   // Run loop cadence (default 1s)
}

type JSONLEncoder[T any] struct{} // dependency-free encoder for tests/light use

type Sink[T any] struct { /* … */ }
func NewSink[T any](cfg Config, decode Decoder[T], enc Encoder[T]) (*Sink[T], error)
func (s *Sink[T]) Poll(ctx) (int, error)      // one ingest cycle; single-goroutine
func (s *Sink[T]) Run(ctx) error              // Poll loop until ctx cancelled
func (s *Sink[T]) VisibleHigh() (uint64, bool) // highest finalized seq + hasData
func (s *Sink[T]) Catalog() []FileInfo

func SegmentStartSeq(ctx, store *wal.Store, segIndex int) (uint64, error) // segment→seq
```

### Behavior (as built)
- **Layout:** `Dir/seq_bucket=%020d/part-%020d-%020d<ext>`, zero-padded for
  lexicographic order; bucket = firstSeq/BucketSize.
- **Batching/flush:** `onRecord` decodes and buffers; flushes at a **bucket
  boundary** (buffer is always single-bucket), or when `MaxRows`/`MaxBytes` hit.
  `Poll` does an **end-of-poll flush** of the remainder so nothing is left
  buffered when caught up. A single `Poll` over seqs 0..24 with BucketSize 10
  yields exactly 3 files (0-9, 10-19, 20-24) — bucket-boundary splitting is
  tested.
- **Atomic publish:** write `<path>.tmp` via the Encoder, then `os.Rename`.
- **Cursor coupling:** the Sink owns the cursor (the Replica is created WITHOUT a
  CursorStore). The cursor advances to `lastFlushed+1` **after** each file is
  finalized, so it reflects Parquet durability, not WAL read progress.
- **Resume:** `NewSink` loads the persisted cursor (overrides StartAt) and scans
  `Dir` to seed `visibleHigh`/`catalog`, so a fresh process knows what's visible
  without re-ingesting. Tested.
- **Idempotency:** range-named files mean a re-ingest of a covered range
  overwrites identically (same names, same bytes). Tested.
- **Error handling:** on `replica.Poll` error, the partial batch is dropped; the
  replica re-delivers from the un-advanced segment next time (at-least-once).
- **Concurrency contract:** drive `Poll`/`Run` from ONE goroutine (single logical
  consumer). `VisibleHigh`/`Catalog` are safe concurrently (RWMutex). Flush I/O
  runs outside the lock; the lock only guards `visibleHigh`/`catalog` for readers.

### parquetenc (nested module — build & test in your env first)
`querystream/parquetenc/parquet.go` implements `querystream.Encoder[T]` with
`parquet.NewGenericWriter[T]`, `SortingColumns(Ascending("seq"))`, and
snappy/zstd. **Contract:** `T` is a struct with `parquet:"…"` tags and a
`seq`-tagged column the Decoder fills from `rec.Sequence`.

### Phase 1 follow-ups for you to do
1. **Compile & test parquetenc** (Go 1.24, `go get github.com/parquet-go/parquet-go`).
   Verify the `WriterOption` calls against the installed version — `MaxRowsPerRowGroup`,
   `SortingWriterConfig`/`SortingColumns`/`Ascending`, and the `compress/snappy`,
   `compress/zstd` import paths may need version-specific tweaks. Add a round-trip
   test: write Parquet, read it back (parquet-go reader or DuckDB), assert schema,
   ascending `seq`, and that row-group statistics carry seq min/max.
2. **`ts` column.** Currently `ts` is just a user column in `T`. `wal.Record`
   does not surface ingestion time (the manifest's `RecordMeta.IngestionTimeMs`
   isn't propagated to `Record`). Options: (a) document that `ts` comes from the
   payload/`GroupMeta` and the Decoder sets it; or (b) small WAL change to add an
   `IngestionTimeMs` field to `wal.Record` and have the replica populate it from
   the entry's `RecordMeta`. (b) is cleaner for time pruning; it's a localized
   change in `wal/replica.go` + `wal/applier.go`.
3. **Optional:** small-file compaction within a bucket (merge `part-*` of one
   `seq_bucket` into one file once a bucket is sealed, i.e. visibleHigh passed
   its upper bound). Keep range-named output; dedup by seq when merging.

---

## 5. Phase 2 — DuckDB query layer (TO BUILD)

New **nested module** `querystream/duckdbq` (own `go.mod`, CGO). Driver:
`github.com/marcboeker/go-duckdb` (database/sql). Isolated exactly like
parquetenc so DuckDB/CGO never touches the WAL/core graph.

### API surface (target)

```go
package duckdbq

type Mode int
const ( Incremental Mode = iota; Cumulative )

type Config struct {
    ReadGlob   string   // e.g. "/data/**/*.parquet" or "s3://bucket/data/**/*.parquet"
    Init       []string // SQL on each connection: LOAD httpfs; CREATE SECRET …; SET threads…
    SeqColumn  string   // default "seq"
    BucketSize uint64   // for the seq_bucket pruning predicate; match the sink
    Dedup      bool     // if true, dedup rows by seq (exactly-once on read)
    View       string   // bound view name, default "records"
}

type Query struct {
    SQL   string // SELECT … FROM records …
    Mode  Mode
    Start uint64 // Cumulative lower bound (0 = all); or resolve from a segment
}

type Engine struct { /* sql.DB + cfg */ }
func Open(cfg Config) (*Engine, error)
func (e *Engine) Close() error

// VisibleHigh derives the highest finalized seq from the read location by
// listing files and parsing part-<first>-<last> names (works local or s3),
// so an out-of-process query knows the window without the Sink.
func (e *Engine) VisibleHigh(ctx) (uint64, bool, error)

// Query runs once over the window for q.Mode and returns rows. For Incremental
// it reads `wm` (the prior watermark) and returns the new high so the caller
// can persist it.
func (e *Engine) Query(ctx, q Query, wm uint64) (rows *sql.Rows, newHigh uint64, err error)

// Stream (in-process convenience) re-runs q whenever a notify channel fires
// (wire it to Sink.VisibleHigh changes), pushing results until ctx is cancelled.
func (e *Engine) Stream(ctx, q Query, notify <-chan uint64) (<-chan *Result, error)
```

### Query construction (the core mechanic)
1. Resolve the window:
   - `hi = visibleHigh` (from `Engine.VisibleHigh` or supplied by the in-process Sink).
   - `lo = wm` for Incremental, `lo = q.Start` (exclusive vs inclusive — pick
     `seq > lo` for Incremental advancing past wm; `seq >= Start` for Cumulative;
     normalize both to `seq > loExclusive` by setting `loExclusive = Start-1`).
2. Build the bound view, lo/hi/bucket inlined as literals:
   ```sql
   CREATE OR REPLACE TEMP VIEW records AS
   SELECT * FROM read_parquet('<glob>', hive_partitioning=true, union_by_name=true)
   WHERE seq > <lo> AND seq <= <hi>
     AND seq_bucket >= <lo / BucketSize>;        -- file-level partition pruning
   ```
   If `Dedup`, wrap so each seq appears once:
   ```sql
   CREATE OR REPLACE TEMP VIEW records AS
   SELECT * EXCLUDE (rn) FROM (
     SELECT *, row_number() OVER (PARTITION BY seq ORDER BY seq) AS rn
     FROM read_parquet(...) WHERE seq > <lo> AND seq <= <hi> AND seq_bucket >= …
   ) WHERE rn = 1;
   ```
   (Or `QUALIFY row_number() OVER (PARTITION BY seq ORDER BY seq) = 1`.)
3. Run the user `q.SQL` against `records`. Return rows; `newHigh = hi`.

### DuckDB setup (Init)
- `INSTALL httpfs; LOAD httpfs;` then a secret. For roles/instance creds:
  `CREATE SECRET (TYPE s3, PROVIDER credential_chain);`. For static:
  `CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', REGION '…');`.
- `SET temp_directory='/tmp'; SET memory_limit='…'; SET threads=<vcpus>;`.
- **Bundle/preload httpfs** if you run somewhere without reliable egress to the
  extension repo (e.g. serverless) — autoinstall is a network dependency.
- For purely local files, no httpfs/secret needed.

### Watermark store (for out-of-process Incremental)
Define `type WatermarkStore interface { Load(ctx)(uint64,bool,error);
SaveCAS(ctx, prev, next uint64) error }`. Back it with `objectstore` conditional
PUT (If-Match) or a file. In-process, a plain field/`MemCursorStore` is fine.

### Tests
- Build Parquet via `parquetenc` from a known seq set; open `duckdbq`; assert:
  - **Cumulative** returns all rows for `seq >= Start`, in order.
  - **Incremental** returns only `wm < seq <= visibleHigh`; watermark advances.
  - **Pruning** fires: `EXPLAIN` / `EXPLAIN ANALYZE` shows files/row-groups
    skipped for a tight `seq` range (and bucket pruning when the predicate is
    injected).
  - **Dedup** collapses an intentionally-overlapping pair of files to one row per
    seq.
  - **Schema evolution**: add a column in later files; `union_by_name` keeps old
    files queryable.
  - **S3 path** (if you have creds): same glob with `s3://` returns identical
    results — proves the read-path config.

---

## 6. Phase 3+ (later, noted for continuity)
- Continuous in-process `Stream` wired to `Sink` notifications; out-of-process
  continuity is caller-triggered (file-arrival event / schedule).
- Compaction of small per-bucket files (matters when flush cadence is high).
- Retention wiring: the Sink's `ingestCursor` is the WAL GC low-watermark — feed
  it to the snapshot/truncate path so raw WAL segments are reclaimed once
  materialized to Parquet (and historical queries hit Parquet, not the WAL tail).
- Arrow fast-path: Arrow C-Data → DuckDB for the freshest, not-yet-flushed rows.
- Promote `querystream` to its own top-level module if desired.

---

## 7. Quick reference: build/run

```bash
# WAL + querystream (sandbox-equivalent)
cd go-buffer && GOFLAGS=-mod=vendor go test -race -count=1 ./...

# parquetenc (your env, Go >= 1.24)
cd go-buffer/querystream/parquetenc && go mod tidy && go test ./...

# duckdbq (after you build it; needs DuckDB/CGO)
cd go-buffer/querystream/duckdbq && go mod tidy && CGO_ENABLED=1 go test ./...
```

The end-to-end smoke test to aim for: produce a WAL → `Sink` with the parquet
encoder writes files → `duckdbq` Cumulative query returns every row in seq order
→ Incremental query returns only the tail after a watermark → both prune by seq.
