# Stream demo - production data flow

How the pieces interact when the producer and consumer run as **separate
processes** against a shared object store (S3/MinIO). The producer is the single
epoch-fenced writer of the WAL; one or more consumers tail it read-only,
materialize local Parquet, and serve queries with DuckDB.

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator / source
    participant P as Producer<br/>(wal.Producer)
    participant S3 as S3 object store<br/>(WAL: segments + manifest)
    participant SK as Consumer · Sink<br/>(wal.Replica tailer)
    participant FS as Local Parquet<br/>(seq_bucket=*/part-*.parquet)
    participant Q as Consumer · duckdbq<br/>(DuckDB engine)
    participant Con as Console / caller

    Note over P,S3: Exactly one epoch-fenced writer per manifest

    rect rgb(235, 245, 255)
    loop per Append batch (CSV rows → records)
        Op->>P: Append(records)
        P->>P: group-commit · seal segment (zstd)
        P->>S3: PUT segment object (immutable)
        P->>S3: CAS manifest (epoch fence · append entry · assign seqs)
        alt CAS wins
            S3-->>P: committed
            P-->>Op: WaitRange → [firstSeq..lastSeq] durable
        else stale epoch
            S3-->>P: precondition failed
            P-->>Op: ErrFenced (a newer primary took over)
        end
    end
    end

    Note over SK,Q: Separate process · read-only on the WAL · many may tail concurrently

    rect rgb(235, 245, 255)
    loop every poll-interval (consumer tick)
        SK->>S3: GET manifest (current entries)
        S3-->>SK: entries past the local cursor
        alt new segments
            SK->>S3: GET new segment object(s)
            S3-->>SK: framed records
            SK->>SK: decode → apply → buffer by seq_bucket<br/>(bounded by MaxRecordsPerPoll)
            SK->>FS: write part-<first>-<last>.parquet (tmp + atomic rename)
            SK->>SK: advance visibleHigh · persist ingest cursor
        else nothing new
            SK-->>SK: no-op (cursor unchanged)
        end

        SK->>Q: run query for this tick
        Q->>FS: glob seq_bucket=*/*.parquet → derive visibleHigh
        Q->>Q: CREATE TEMP VIEW bounded to the window<br/>cumulative [start, hi] · incremental [wm, hi]
        Q->>FS: read_parquet(window) - seq_bucket pruning + optional dedup
        FS-->>Q: rows
        Q-->>Con: render result (table refresh)
        Note over Q: incremental: advance watermark to hi+1<br/>(half-open next-to-read 0 = nothing read)
    end
    end
```

## Reading the diagram

**Producer (write path, steps 1–9).** Each `Append` group-commits, seals a
segment, and writes it to the object store as an immutable blob *before* a
compare-and-swap on the manifest. The CAS is epoch-fenced: a stale primary whose
epoch was bumped by a newer one gets `ErrFenced` on its next commit, so at most
one writer ever advances the log. Per-record sequence numbers are assigned at
manifest-append time; `WaitRange` returns the durable `[first..last]` range.

**Consumer (read path, the poll loop).** A `wal.Replica` inside the `Sink` reads
the manifest, fetches only segments past its local cursor, decodes the framed
records, and buffers them by `seq_bucket`. Completed buckets are written as
Parquet via temp-file + atomic rename, then `visibleHigh` and the ingest cursor
advance - the cursor only moves once rows are durable on disk, so a crash
re-ingests at-least-once (read-side dedup on `seq` makes that exactly-once).
`MaxRecordsPerPoll` optionally paces ingestion so the view builds up gradually.

**Query (the trigger).** After ingesting, the same tick drives `duckdbq`: it
re-globs the Parquet directory to derive the current `visibleHigh` (so files the
Sink just wrote appear automatically), defines a TEMP VIEW bounded to the query
window, and runs the caller's SQL over `read_parquet` - with `seq_bucket`
partition pruning and optional dedup. Cumulative re-aggregates over
`[start, visibleHigh]` each tick (running totals); incremental returns only
`[watermark, visibleHigh]` and advances the half-open watermark to
`visibleHigh+1`.

## Production notes

- **One writer, many readers.** The manifest CAS fences writers; readers are not
  fenced, so any number of consumers can tail the same WAL, each maintaining its
  own local Parquet + cursor. Lag is ~one poll interval.
- **Object store is the source of truth.** Segments are immutable; the manifest
  is the only mutable object and is updated only by CAS. Local Parquet is a
  derived, rebuildable cache.
- **Same code path over `s3://`.** This demo keeps Parquet local, but pointing
  `duckdbq.Config.ReadGlob` at `s3://…` (with `httpfs` loaded) lets a separate
  query process read files a consumer uploaded - the query layer re-globs either
  way.
