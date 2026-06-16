# objwal

A replication write-ahead log built on object-storage queue primitives.
The log lives entirely in a bucket: segment objects hold the framed records, and one manifest object,
mutated with conditional puts (CAS), is the broker-less ordering authority.

There is no broker, no consensus service, and no shared block device - just an
object store that supports conditional writes (S3 `If-Match`/`If-None-Match`).

## Lineage: from the opendata buffer

objwal descends from a Go port of the Rust [opendata-buffer](https://github.com/opendata-oss/opendata/tree/main/buffer). All credit
should go to the team at pendata.

A stateless, broker-less queue over object storage, where producers append
batches and a single fenced consumer destructively dequeues them. We kept the
hard-won primitives (the CAS manifest, ULID-named data objects, epoch fencing,
optimistic-concurrency append) and reshaped the queue into a non-destruct log:

| opendata buffer (queue) | objwal (replication log) |
|---|---|
| Consumer dequeues (`AckThrough` deletes entries) | Replicas tail read-only; entries are retained, GC trims by retention window |
| N stateless producers contend on the manifest | One epoch-fenced primary writes; failover bumps the epoch |
| Consumer is the manifest owner | Many replicas read the same manifest concurrently (readers aren't fenced) |
| Batch = opaque entries for a worker to drain | Segment = framed records a replica applies via an `Applier` |
| Manifest footer v1 | Footer v2 adds a snapshot pointer (v1 still parses) |

The wire formats are deliberately byte-compatible: a segment is the buffer's
batch-v1 record block, and the v2 manifest footer is a strict superset of v1, so
a replica can even bootstrap from a plain upstream buffer manifest.

## Topology

```text
        PRIMARY (1, epoch-fenced)                 Object store bucket                 REPLICAS (N, read-only)
  ┌──────────────────────────────┐        ┌────────────────────────────────┐      ┌────────────────────────────┐
  │ local store  ◄── double-write│        │ wal/seg/<run>/0000…0  (segment)│ poll │ Replica.Poll/Run           │
  │                              │ upload │ wal/seg/<run>/0000…1  (segment)│ ───► │  → Applier.Apply(record)   │
  │ Producer.Append() ───────────┼──────► │ …                              │ get  │  → local store (full copy) │ ──► reads
  │           └─ group-commit ───┼─ CAS ► │ wal/manifest  ◄── ordering log │ ───► │  → advance cursor          │
  └──────────────────────────────┘        └────────────────────────────────┘      └────────────────────────────┘
```

Replicas materialize a full local copy and serve reads locally, lagging the
primary by roughly one flush interval.

## Timeline of writes and commits

A write travels through Append → buffer → flush (seal + upload) → commit (manifest CAS),
and only then becomes visible to replicas. Uploads parallelize *within* a flush;
the manifest commit is strictly serial and is what assigns the total order.

```text
PRIMARY
  Append(r0) ─┐
  Append(r1) ─┼─► [ pending buffer ]
  Append(r2) ─┘        │ flush trigger: FlushInterval elapsed OR FlushBytes reached
                       ▼
                 plan segments (pack groups ≤ SegmentMaxBytes, reserve ordinals)
                       │
        ┌──────────────┴───────────────┐   uploads run concurrently (UploadConcurrency)
        ▼              ▼                ▼
     PUT seg0       PUT seg1        PUT seg2        ← object writes, unordered, idempotent
        └──────────────┬───────────────┘
                       ▼
        commit in ORDINAL order  ── manifest CAS ──►  seq0, seq1, seq2 assigned   ← single linearization point
                       │                              (coalesce ≤ ManifestAppendBatchSize per CAS)
                       ▼
              Durability.Wait() returns seq   (record is now durable + ordered)

REPLICA  (independently, every PollInterval)
  load manifest ─► EntriesFrom(cursor) ─► GET segN ─► decode ─► Apply each record ─► cursor = seqN + 1
```

Two things set the throughput/latency envelope:

- **Per-flush parallelism is real**: segment uploads overlap (`UploadConcurrency`)
  and multiple entries coalesce into one manifest CAS (`ManifestAppendBatchSize`).
- **Flush boundaries are a serialization point**: the flush loop runs one flush
  at a time, so flush *N*'s manifest commit does not overlap flush *N+1*'s
  uploads. Overlapping them (a persistent cross-flush committer) is the main
  remaining write-throughput lever; the replica tailer fetching segments
  sequentially is the read-side one. Both are tracked in the architecture doc.

Order is defined by the manifest commit, never by upload completion. Replicas
only ever read a segment *after* its manifest entry exists, so out-of-order or
early uploads are invisible until committed, the log a replica sees is always
linear.

## Usage

### Producer (the primary)

```go
import (
	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

store := objectstore.NewInMemory() // or objectstore.NewS3(client, bucket)

p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
	ManifestPath:  "wal/manifest",
	SegmentPrefix: "wal/seg",
	FlushInterval: 50 * time.Millisecond,
	FlushBytes:    8 << 20, // seal a segment early at 8 MiB buffered
})
if err != nil { ... } // wal.ErrFenced => a newer primary already claimed the log

// Records are opaque framed bytes (the frame format is your own).
// meta is an optional payload attached to this Append group. Append BLOCKS once
// the in-flight byte budget is exhausted, that is the backpressure signal.
d, err := p.Append(ctx, [][]byte{frame0, frame1}, []byte("optional-meta"))
if err != nil { ... }

seq, err := d.Wait(ctx) // blocks until the group is durably committed
if err != nil { ... }   // wal.ErrFenced / wal.ErrHalted on failover or fatal error

p.Close(ctx) // flush remaining records and halt
```

Multiple concurrent `Append`s coalesce into segments and commit together.
`NewProducer` claims the log by bumping the manifest epoch; if a newer primary
later claims it, this one's next commit detects the epoch change and halts with
`ErrFenced`. Failover is just constructing a fresh producer.

### Replica (a reader)

The replica is system-agnostic: it hands each framed record to an `Applier`.
The recommended `TypedApplier` splits that into a decoder (bytes → your op) and
a handler (apply the op to your system):

```go
applier := wal.TypedApplier(
	decodeOp, // func([]byte) (Op, error) mirrors however you framed the record
	func(ctx context.Context, seq uint64, op Op) error {
		return db.Apply(op) // your Bitcask / SQLite / cache / KV store
	},
)

r := wal.NewReplica(store, applier, wal.ReplicaConfig{
	ManifestPath: "wal/manifest",
	Cursor:       wal.NewFileCursorStore("/var/lib/app/wal.cursor"), // resume across restarts
})

// Tail until ctx is cancelled (or call r.Poll(ctx) yourself to step it).
if err := r.Run(ctx); err != nil { ... }
```

`Apply` must be idempotent: produce is at-least-once and the tailer
re-applies a whole segment if an earlier record in it failed or the cursor
wasn't persisted before a crash, so the same record may arrive more than once.
Idempotent `put`/`delete` satisfies this with no extra state. For a quick
in-line applier without the typed split, use `wal.ApplyFunc`.

## Intended use & use cases

objwal is the replication substrate for a single-writer storage engine that
wants cheap, durable, broker-less read replicas you bring the engine and the
record frame; objwal owns ordering, durability, and fan-out through a bucket.

- **Read replicas for an embedded KV / log-structured store** (Bitcask, an LSM,
  a custom append-only engine): the primary double-writes, replicas rebuild a
  full local copy and serve low-latency local reads.
- **Cross-AZ / cross-region durability without inter-AZ traffic**: data flows
  through the object store, not across zones, and survives a primary crash.
- **Decoupling writes from slow readers**: a lagging or restarting replica just
  resumes from its cursor; it never back-pressures the primary beyond the
  bucket's own throughput.
- **Disaster recovery / point-in-time rebuild**: a replica can bootstrap from a
  base snapshot (manifest footer v2) and tail forward.

It is not a general message queue, not multi-writer, and not exactly-once on
the producer side. One log = one writer; run many independent
logs for aggregate throughput.

## Status

Probably don't use in production. There are tests, they could be wrong.

## Wire formats

All integers little-endian. Segment (`<prefix>/<runID>/<ordinal:016x>`):

```text
record block (optionally zstd):  [len u32][data] ...
footer (7 B):  compression u8 | record_count u32 | version u16 (=1)
```

Manifest (read from the end; the v2 snapshot block is omitted in v1):

```text
entry: [body_len u32][sequence u64][loc_len u16][loc][md_count u32]
       per md: [start_index u32][ingestion_ms i64][payload_len u32][payload]
snapshot block (v2 only): [loc bytes][loc_len u16][through_seq u64][created_ms i64]
footer (22 B): entries_count u32 | next_sequence u64 | epoch u64 | version u16 (=2)
```

The in-memory `Manifest` keeps appends O(1) (side buffer merged at serialize
time); `TruncateThrough` drops a superseded prefix at snapshot cadence.

## Benchmarks & tests

```bash
go test -race ./...                 # unit + integration, race-clean
./scripts/test.sh                   # end-to-end WAL throughput over local MinIO
BENCH_LATENCY=true ./scripts/test.sh   # per-record commit latency (p50/p90/p99)
```

`cmd/s3bench` drives the real producer/replica path (not raw `objectstore`
PUT/GET) and reports write/read throughput in Mbps or commit-latency
percentiles; `scripts/test.sh` documents the tuning knobs and recipes for
trading throughput against latency.
