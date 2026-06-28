// Package querystream is a consumer that materializes a replication-WAL stream
// into seq-partitioned, seq-sorted column files for an external query engine
// (DuckDB) to read. It tails the WAL via the wal.Replica/Applier, decodes
// each opaque record with a caller-supplied Decoder, batches rows, and writes
// finalized files whose names encode the record-sequence range they cover.
//
// The on-disk layout is:
//
//	<Dir>/seq_bucket=<bucket>/part-<firstSeq>-<lastSeq><ext>
//
// where bucket = firstSeq / BucketSize. Files are sorted by sequence and carry
// a seq column, so a reader prunes by row-group statistics (every WHERE seq>=X)
// and, optionally, by the seq_bucket hive partition.
//
// The file encoding is pluggable via Encoder. The core package ships a
// dependency-free JSONLEncoder for tests and light use; the Parquet encoder
// lives in the nested ./parquetenc module so the heavy parquet-go dependency
// (and, later, the DuckDB/CGO query layer) never enters the WAL's dependency
// graph.
//
// This package is the write/ingest half. It is single-writer: drive Poll/Run
// from one goroutine (one logical consumer per dataset). VisibleHigh, Catalog,
// and any future query call are safe to call concurrently.
package querystream

import (
	"bufio"
	"encoding/json"
	"os"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// Decoder turns one opaque WAL record into a typed row. It is the caller's
// "supplier" decoder: it mirrors however the producer framed the record, and it
// has access to the full wal.Record (sequence, group metadata, bytes). The
// sink tracks rec.Sequence itself for partitioning; by convention the row type
// should also carry that sequence in a "seq" field so it lands in the output
// file as the addressable primary key.
type Decoder[T any] func(rec wal.Record) (T, error)

// Encoder writes a finalized, seq-sorted batch of rows to a single file. seqs[i]
// is the record sequence of rows[i] (already ascending). Implementations write
// to exactly the given path and must not return until the bytes are flushed to
// it; the sink handles atomic publish (temp file + rename) around this call.
type Encoder[T any] interface {
	Encode(path string, seqs []uint64, rows []T) error
	// Ext is the file extension the encoder produces, including the dot,
	// e.g. ".parquet" or ".jsonl".
	Ext() string
}

// FileInfo describes one finalized output file.
type FileInfo struct {
	Path     string
	Bucket   uint64
	FirstSeq uint64
	LastSeq  uint64
	Rows     int
}

// Config wires the WAL source, the local output layout, and the flush policy.
type Config struct {
	// WAL source.
	ObjectStore  objectstore.ObjectStore
	ManifestPath string
	// StartAt is the first record sequence to ingest when no cursor is
	// persisted (0 = from the beginning of what the manifest still holds).
	StartAt uint64
	// Cursor persists ingest progress (next sequence to read). A persisted
	// value overrides StartAt, giving resumability across restarts. The cursor
	// only advances after the corresponding rows are durably in a finalized
	// file, so it doubles as the retention low-watermark for WAL GC.
	Cursor wal.CursorStore

	// Output (local filesystem).
	Dir        string // root directory for output files
	BucketSize uint64 // record sequences per partition bucket (default 1<<20)

	// Flush policy. A file is finalized at a bucket boundary, or when one of
	// these caps is reached, whichever comes first.
	MaxRows  int // rows per file (default 50_000)
	MaxBytes int // approx source-bytes per file (0 = disabled)

	// PollInterval is how often Run polls the WAL for new records (default 1s).
	PollInterval time.Duration

	// MaxRecordsPerPoll bounds how many WAL records one Poll ingests, stopping at
	// the next segment boundary once met (0 = unbounded). Paces ingestion so a
	// pre-loaded WAL materializes gradually across polls rather than all at once.
	MaxRecordsPerPoll int
}

func (c *Config) withDefaults() {
	if c.BucketSize == 0 {
		c.BucketSize = 1 << 20
	}
	if c.MaxRows <= 0 {
		c.MaxRows = 50_000
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
}

// JSONLEncoder writes one JSON object per line: {"seq":<n>,"row":<row>}. It has
// no dependencies beyond the standard library and is used for tests and light
// workloads. For analytical querying use the Parquet encoder in ./parquetenc.
type JSONLEncoder[T any] struct{}

// Ext implements Encoder.
func (JSONLEncoder[T]) Ext() string { return ".jsonl" }

// Encode implements Encoder.
func (JSONLEncoder[T]) Encode(path string, seqs []uint64, rows []T) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range rows {
		rec := struct {
			Seq uint64 `json:"seq"`
			Row T      `json:"row"`
		}{seqs[i], rows[i]}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return w.Flush()
}
