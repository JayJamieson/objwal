// Package duckdbq is the read/query half of querystream: a stateless DuckDB
// reader over the seq-partitioned, seq-sorted Parquet files that the
// querystream.Sink materializes from a replication WAL.
//
// It never parses user SQL. For each query it derives a [lo,hi] record-sequence
// window from the query mode and the visible-high watermark, defines a bounded
// TEMP VIEW (default name "records") that selects the windowed rows from
// read_parquet over the file glob, then runs the caller's SQL against that view.
// Because it re-globs on every query, files written by the Sink after Open
// appear automatically, and the same glob works over a local path or s3:// (via
// httpfs) unchanged.
//
// This is an isolated nested module (own go.mod) so DuckDB and its CGO/native
// dependency never enter the WAL/core build graph; consumers of the WAL that do
// not query keep a pure-Go, CGO-free build.
package duckdbq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Mode selects how a query's lower bound is resolved.
type Mode int

const (
	// Incremental returns rows from the supplied watermark through visibleHigh:
	// queryWatermark <= seq <= visibleHigh. The watermark is the next sequence to
	// read (half-open, like the replica cursor): 0 means "nothing read yet" and
	// reads from the first record; it advances to visibleHigh+1 after an emit.
	Incremental Mode = iota
	// Cumulative returns every row from Query.Start through visibleHigh,
	// re-scanning on each call: Start <= seq <= visibleHigh.
	Cumulative
)

// ErrNoData is returned by Query when the read location holds no finalized
// files yet (nothing to query). Callers driving an Incremental loop should treat
// it as "no data yet" and retry later.
var ErrNoData = errors.New("duckdbq: no finalized files in read location")

// Config wires the read location and the DuckDB session.
type Config struct {
	// ReadGlob is the parquet file glob, e.g. "/data/seq_bucket=*/*.parquet"
	// or "s3://bucket/data/seq_bucket=*/*.parquet". It is module-controlled and
	// inlined into SQL; do not source it from untrusted input.
	ReadGlob string
	// Init runs on the (single) DuckDB connection at Open time, e.g.
	// "INSTALL httpfs; LOAD httpfs;", "CREATE SECRET (...)", "SET threads=8".
	Init []string
	// SeqColumn is the per-record sequence column (default "seq"); it is the
	// primary key, the window predicate column, and the dedup key.
	SeqColumn string
	// BucketSize is the sink's seqs-per-bucket; it must match the sink so the
	// query layer can inject the seq_bucket lower bound for partition pruning.
	BucketSize uint64
	// Dedup, when true, collapses duplicate rows by SeqColumn (exactly-once on
	// read over an at-least-once ingest that may overlap seq ranges across files).
	Dedup bool
	// View is the bound view name user SQL selects from (default "records").
	View string
}

func (c *Config) withDefaults() {
	if c.SeqColumn == "" {
		c.SeqColumn = "seq"
	}
	if c.View == "" {
		c.View = "records"
	}
	if c.BucketSize == 0 {
		c.BucketSize = 1 << 20
	}
}

// Query is a single read against the bound view.
type Query struct {
	// SQL selects FROM the bound view (Config.View), e.g.
	// "SELECT seq, val FROM records ORDER BY seq". It is run verbatim.
	SQL string
	// Mode selects the lower-bound resolution (see Mode).
	Mode Mode
	// Start is the Cumulative inclusive lower bound (0 = from the beginning).
	// Ignored for Incremental (the watermark argument is the lower bound).
	Start uint64
}

// Engine is a stateless query engine over a fixed read location.
type Engine struct {
	db  *sql.DB
	cfg Config
}

// Open builds an Engine. The DuckDB database is in-memory (it is only a reader
// over the files); Init statements run once on the pinned connection.
func Open(cfg Config) (*Engine, error) {
	if cfg.ReadGlob == "" {
		return nil, fmt.Errorf("duckdbq: ReadGlob is required")
	}
	cfg.withDefaults()
	if !validIdent(cfg.View) {
		return nil, fmt.Errorf("duckdbq: invalid View name %q", cfg.View)
	}
	if !validIdent(cfg.SeqColumn) {
		return nil, fmt.Errorf("duckdbq: invalid SeqColumn name %q", cfg.SeqColumn)
	}

	init := append([]string(nil), cfg.Init...)
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		for _, stmt := range init {
			if _, err := execer.ExecContext(context.Background(), stmt, nil); err != nil {
				return fmt.Errorf("duckdbq: init %q: %w", stmt, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("duckdbq: connector: %w", err)
	}
	db := sql.OpenDB(connector)
	// A TEMP VIEW is connection-scoped and Init runs per new connection. Pin the
	// pool to a single connection so the bounded view created for a query is
	// visible to that query's SQL, and Init runs exactly once.
	db.SetMaxOpenConns(1)
	return &Engine{db: db, cfg: cfg}, nil
}

// Close releases the underlying database.
func (e *Engine) Close() error { return e.db.Close() }

// DB exposes the underlying *sql.DB for advanced callers (e.g. EXPLAIN in
// tests). The bound view only exists immediately after a Query call on the same
// pinned connection.
func (e *Engine) DB() *sql.DB { return e.db }

// validIdent reports whether s is a safe bare SQL identifier (the view/column
// names are inlined unquoted).
func validIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		ok := r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(i > 0 && r >= '0' && r <= '9')
		if !ok {
			return false
		}
	}
	return true
}

// sqlString renders s as a single-quoted SQL string literal.
func sqlString(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	out = append(out, '\'')
	return string(out)
}
