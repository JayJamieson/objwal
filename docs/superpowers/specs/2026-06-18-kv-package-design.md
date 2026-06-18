# `kv` — a thin bitcask-style key-value store over objwal

Date: 2026-06-18
Status: Approved (demo scope)

## Purpose

A small `kv` package that turns the objwal write-ahead-log primitives into a
usable key-value store with `Get` / `Put` / `Delete`. It is a **thin wrapper**:
objwal owns durability, ordering, and replication; `kv` owns op framing, an
in-memory keydir, and a local append-only file that backs low-latency reads.

The design follows bitcask: an in-memory map (the *keydir*) holds each key's
location, and the values themselves live in an append-only file on local disk.
objwal is the durable, replicated source of truth; the local file is a derived
read/recovery cache.

This is a **demo**, not production storage. Several correctness corners are
deliberately left as documented constraints (see [Known limitations](#known-limitations)).

## Two modes

| Mode | Constructor | Operations | Backed by |
|---|---|---|---|
| **Primary** (read-write) | `kv.Open(ctx, store, cfg)` → `*DB` | `Get`, `Put`, `Delete`, `Close` | `wal.Producer` + local file |
| **Replica** (read-only) | `kv.OpenReplica(ctx, store, cfg)` → `*Replica` | `Get`, `Poll`, `Run`, `Close` | `wal.Replica` + local file |

Both wrap the same unexported core (`local`: keydir + append-only file + the
apply path). The primary writes through `Put`/`Delete`; the replica writes
through its `Applier` while tailing objwal. The two write paths converge on the
**same** `local.apply` function, so a key looks identical on the primary and on
every replica.

## Data structures

### keydir (in memory)

```go
type loc struct {
    off int64  // absolute file offset of the value bytes
    len uint32 // value length
}

type local struct {
    mu     sync.RWMutex
    keydir map[string]loc // key string -> value location
    f      *os.File
    tail   int64          // current end-of-file offset
    path   string
}
```

A `DELETE` removes the key from `keydir` entirely; `Get` on a missing key is a
clean miss.

### Local file record framing

Append-only. Each record is length-framed and crc-checked so a torn trailing
write (no `fsync`, see limitations) is detectable on recovery:

```
[recLen u32]   bytes that follow, up to and including crc
[op    u8]     0 = PUT, 1 = DELETE
[klen  u32]
[key   klen bytes]
[value vlen bytes]   vlen = recLen - 1 - 4 - klen - 4   (0 for DELETE)
[crc32 u32]    CRC-32 (IEEE) over op..value
```

The keydir's `off` points at the start of `[value]`; `len` is `vlen`. `Get` is a
single `ReadAt` of `len` bytes at `off`. All integers little-endian, matching the
rest of the repo.

### objwal record frame (the opaque bytes handed to the WAL)

objwal records are opaque; this is `kv`'s own frame, decoded by replicas:

```
[op u8][klen u32][key][value]   value is the remaining bytes (empty for DELETE)
```

## Write path (primary)

```go
func (db *DB) Put(ctx context.Context, key, value []byte) error {
    frame := encodeOp(opPut, key, value)
    d, err := db.prod.Append(ctx, [][]byte{frame}, nil) // producer is thread-safe; no kv lock here
    if err != nil { return err }
    if _, err := d.Wait(ctx); err != nil { return err } // block until objwal-committed
    return db.local.apply(opPut, key, value)            // brief lock: append file + update keydir
}
```

`Delete` is identical with `opDelete` and a nil value.

Key properties:

- **No kv-level lock around the objwal write.** `wal.Producer.Append` is already
  thread-safe and assigns objwal order. The `kv` mutex is taken only inside
  `local.apply`, guarding the file append and the keydir map.
- **The slow commit happens lock-free**, so concurrent `Put`/`Delete` coalesce
  into one objwal group commit and never block one another — the mutex behaviour
  the design called for.
- **Returns only after objwal commit**, so a successful `Put`/`Delete` means the
  op is durably replicated, and the value is already in the local file and
  keydir before the call returns (read-your-writes for the calling goroutine).

`local.apply` (under `mu.Lock()`): serialize the record, write it at `tail`,
compute the value offset, set or delete the keydir entry, advance `tail`.

## Read path

```go
func (db *DB) Get(key []byte) ([]byte, bool, error)
```

`RLock`, look up the keydir (miss → `(nil, false, nil)`), `ReadAt` the value
region, `RUnlock`. `ReadAt` (pread) targets an already-written, immutable region
and does not touch the write offset, so reads never contend with the tail append
beyond the brief keydir read-lock.

## Replica mode

`OpenReplica` builds a `wal.Replica` whose `Applier` decodes each frame and calls
the same `local.apply`:

```go
applier := wal.ApplyFunc(func(ctx context.Context, rec wal.Record) error {
    op, key, value, err := decodeOp(rec.Data)
    if err != nil { return err }
    return l.apply(op, key, value) // idempotent: re-applying a put/delete is safe
})
rep := wal.NewReplica(store, applier, wal.ReplicaConfig{
    ManifestPath: cfg.ManifestPath,
    PollInterval: cfg.PollInterval,
    Cursor:       wal.NewFileCursorStore(cfg.CursorPath), // resume across restarts
})
```

`Run(ctx)` tails until cancelled; `Poll(ctx)` steps once. The replica is
read-only: it exposes `Get` (same as the primary) and no `Put`/`Delete`.

## Recovery (on open)

Implements the agreed rule: **check local, otherwise replay objwal.**

- **Primary** `Open`:
  1. Open the local file and scan it front-to-back, rebuilding the keydir
     (last record per key wins; `DELETE` removes). A short read or crc mismatch
     on the final record means a torn tail → truncate the file there and stop.
  2. If the local file did not exist / had no records, replay objwal from
     sequence 0 through a one-shot `wal.Replica` (same `Applier`) to rebuild the
     local file and keydir before accepting writes.
  3. Construct the `wal.Producer` (claims the log, bumps the epoch).
- **Replica** `OpenReplica`: scan the local file for the keydir as above, then
  let the underlying `wal.Replica` resume tailing from its persisted cursor
  (or from 0 if none). Re-applied segments are idempotent.

## Public API surface

```go
package kv

type Config struct {
    ManifestPath  string        // objwal manifest object key
    SegmentPrefix string        // objwal segment key prefix (primary)
    LocalPath     string        // local append-only data file
    FlushInterval time.Duration // forwarded to wal.ProducerConfig (primary)
    FlushBytes    int           // forwarded to wal.ProducerConfig (primary)
}

type ReplicaConfig struct {
    ManifestPath string
    LocalPath    string
    CursorPath   string        // defaults to LocalPath + ".cursor" when empty
    PollInterval time.Duration
}

func Open(ctx context.Context, store objectstore.ObjectStore, cfg Config) (*DB, error)
func (db *DB) Get(key []byte) (value []byte, found bool, err error)
func (db *DB) Put(ctx context.Context, key, value []byte) error
func (db *DB) Delete(ctx context.Context, key []byte) error
func (db *DB) Close(ctx context.Context) error

func OpenReplica(ctx context.Context, store objectstore.ObjectStore, cfg ReplicaConfig) (*Replica, error)
func (r *Replica) Get(key []byte) (value []byte, found bool, err error)
func (r *Replica) Poll(ctx context.Context) (int, error)
func (r *Replica) Run(ctx context.Context) error
func (r *Replica) Close() error
```

Keys and values are `[]byte`.

## Known limitations (documented, not fixed in v1)

- **Same-key concurrent writes resolve in commit-completion order, not objwal
  order.** Two goroutines writing the *same* key concurrently update the local
  keydir after each `Wait()` returns, in goroutine-wakeup order. The objwal (and
  therefore replicas) order them by sequence; the primary's local keydir may
  briefly reflect the other one. **Constraint: do not write the same key
  concurrently from multiple goroutines; if you do, last-writer-wins is by
  completion order and the primary may diverge from replicas for that key.**
  Different keys are unaffected and fully concurrent.
- **No `fsync` on the local file.** objwal commit is the durable ack; the local
  file is a derived cache. A primary crash can lose a just-committed tail that
  reached objwal but not the local file, and "trust local on restart" will not
  re-pull it. Acceptable for a demo.
- **Replica restarts may re-append already-applied records** to the local file
  (idempotent in the keydir, but they accumulate as dead bytes), and the local
  file may lag the persisted cursor under the no-`fsync` caveat above.
- **No compaction / GC** of the local file; overwrites and deletes leave dead
  bytes. No bitcask-style merge.
- **No snapshot bootstrap**; cold start replays objwal from sequence 0. The
  manifest footer v2 snapshot pointer exists for this as future work.

## Testing strategy

All tests use `objectstore.NewInMemory()` and a `t.TempDir()` local file.

- **op framing**: `encodeOp` / `decodeOp` round-trip, including empty value
  (delete) and binary keys/values.
- **local file**: `apply` then `get`; overwrite a key; delete a key; reopen and
  confirm the keydir rebuilds by scanning; a manually truncated/corrupted final
  record is dropped on rescan.
- **primary**: `Put`/`Get`/`Delete` happy paths; reopen `DB` and confirm data
  survives (local-file recovery); open a fresh `DB` against a store that already
  has objwal data and confirm objwal replay rebuilds it; concurrent `Put`s to
  *different* keys.
- **replica**: primary writes N keys; a replica over the same in-memory store
  tails and `Get` returns the same values; a delete on the primary becomes a
  miss on the replica after polling.

## Out of scope

Compaction/merge, snapshots, multi-writer, fsync durability of the local file,
range scans / iteration, and TTL. Each is a clean future extension.
