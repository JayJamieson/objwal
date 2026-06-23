# cmd/stream - query-streaming demo

End-to-end demo of the objwal stack: a CSV is streamed into the epoch-fenced WAL
(stored in MinIO), tailed by `querystream.Sink` into local Parquet, and queried
live with DuckDB through `duckdbq`, re-rendered in place each refresh.

```
CSV ─producer─▶ WAL ─▶ MinIO ─consumer:Sink─▶ Parquet ─duckdbq─▶ SQL ─▶ console
```

See [SPEC.md](SPEC.md) for the full design and [FLOW.md](FLOW.md) for a sequence
diagram of the producer → S3 WAL → consumer → Parquet → query path.

## This is a separate module

To keep the CGO/DuckDB dependency out of the pure-Go objwal root build graph
(the same reason `querystream/duckdbq` is nested), this demo has its own
`go.mod` with `replace` directives back to the root and to `duckdbq`. Build with
`CGO_ENABLED=1`, **from this directory** (not `go run ./cmd/stream` at the repo
root - different module):

```bash
cd cmd/stream
go build -o /tmp/streamdemo .     # CGO_ENABLED=1 is the default
```

## Run

Start MinIO:

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data
```

Intended workflow: **load the WAL once with the producer, then run the consumer**
(which follows by default). To *watch* the view build up from an already-loaded
WAL, pace the consumer with `-ingest-per-tick` so it ingests a few records per
refresh instead of draining everything on the first tick.

```bash
# 1 - load the CSV into the WAL once. Small segments give the consumer
#     boundaries to pace at (-ingest-per-tick stops at a segment boundary).
go run . -mode producer -csv reviews.csv -append-delay 0s -segment-max-bytes 16

# 2a - FULL mode: average rating per product, climbing as rows are ingested.
go run . -mode consumer -reset -ingest-per-tick 2 -poll-interval 1s \
  -sql "SELECT product_name, COUNT(*) AS n, ROUND(AVG(rating),2) AS avg_rating
        FROM records GROUP BY product_name ORDER BY n DESC, product_name"

# 2b - INCREMENTAL mode: only the rows materialized since the last refresh.
go run . -mode consumer -reset -incremental -ingest-per-tick 2 -poll-interval 1s \
  -sql "SELECT seq, product_name, rating, substr(review_text,1,48) AS snippet
        FROM records ORDER BY seq"
```

Drop `-ingest-per-tick` to ingest everything on the first tick (the view jumps
straight to its final state). Add `-follow=false` for a one-shot: drain the WAL,
render once, exit. `-config <file>` supplies JSON defaults that any explicit flag
overrides; see `demo.example.json` and `SPEC.md`'s flag reference.

### Consumer pacing flags

| flag | default | effect |
|---|---|---|
| `-follow` | true | loop, refreshing each `-poll-interval`; `false` = one-shot |
| `-ingest-per-tick` | 0 | cap WAL records ingested per tick (0 = drain all). Stops at the next **segment** boundary once met, so load the WAL with small `-segment-max-bytes` for fine granularity. |

## Notes

- **seq vs Index:** `seq` is the WAL's per-record sequence (0-based, global, the
  PK the query layer prunes/dedups on); the CSV `Index` is just a payload column.
- **Incremental watermark** is half-open (next-to-read): a fresh consumer starts
  at `wm=0` and sees every row including `seq 0`; each tick advances it to
  `visibleHigh+1`, so a no-new-data tick renders an empty `[hi+1, hi]` window.
- **Single writer:** run exactly one producer per manifest (the WAL is
  epoch-fenced). The consumer is read-only on the WAL.
