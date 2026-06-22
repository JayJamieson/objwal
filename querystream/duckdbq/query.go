package duckdbq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Query resolves the [lo,hi] sequence window for q.Mode, defines the bounded
// view, and runs q.SQL against it. It returns the result rows and newHigh (the
// window's upper bound = current visibleHigh); for Incremental, persist newHigh
// as the next watermark after consuming the rows.
//
// wm is the prior Incremental watermark (the exclusive lower bound); it is
// ignored for Cumulative, which uses q.Start. Returns ErrNoData when the read
// location holds no finalized files.
func (e *Engine) Query(ctx context.Context, q Query, wm uint64) (*sql.Rows, uint64, error) {
	hi, have, err := e.VisibleHigh(ctx)
	if err != nil {
		return nil, 0, err
	}
	if !have {
		return nil, 0, ErrNoData
	}

	loExcl, hasLo, bucketLo := resolveWindow(q, wm, e.cfg.BucketSize)
	view := e.buildViewSQL(loExcl, hasLo, hi, bucketLo)
	if _, err := e.db.ExecContext(ctx, view); err != nil {
		return nil, 0, fmt.Errorf("duckdbq: build view: %w", err)
	}
	rows, err := e.db.QueryContext(ctx, q.SQL)
	if err != nil {
		return nil, 0, fmt.Errorf("duckdbq: query: %w", err)
	}
	return rows, hi, nil
}

// resolveWindow maps (mode, watermark, Start) to an exclusive lower bound and
// the seq_bucket lower bound for partition pruning. Cumulative with Start==0 has
// no lower bound (full scan, no bucket predicate so flat layouts also work).
func resolveWindow(q Query, wm, bucketSize uint64) (loExcl uint64, hasLo bool, bucketLo uint64) {
	switch q.Mode {
	case Incremental:
		return wm, true, wm / bucketSize
	default: // Cumulative
		if q.Start == 0 {
			return 0, false, 0
		}
		loExcl = q.Start - 1
		return loExcl, true, loExcl / bucketSize
	}
}

// buildViewSQL renders the CREATE OR REPLACE TEMP VIEW statement that bounds the
// read to (loExcl, hi] (or [.., hi] when !hasLo), injects the seq_bucket lower
// bound for file-level pruning, and optionally dedups by the seq column.
func (e *Engine) buildViewSQL(loExcl uint64, hasLo bool, hi, bucketLo uint64) string {
	seq := e.cfg.SeqColumn
	var where strings.Builder
	fmt.Fprintf(&where, "%s <= %d", seq, hi)
	if hasLo {
		fmt.Fprintf(&where, " AND %s > %d", seq, loExcl)
		// seq_bucket is the sink's hive partition column; bounding it lets DuckDB
		// prune whole partitions it would not derive from a seq predicate alone.
		fmt.Fprintf(&where, " AND seq_bucket >= %d", bucketLo)
	}

	// hive_types forces seq_bucket numeric (it is otherwise inferred VARCHAR from
	// the zero-padded directory value) so the >= bound compares and prunes
	// numerically, independent of the directory zero-padding width.
	base := fmt.Sprintf(
		"SELECT * FROM read_parquet(%s, hive_partitioning=true, hive_types={'seq_bucket': 'UBIGINT'}, union_by_name=true) WHERE %s",
		sqlString(e.cfg.ReadGlob), where.String())
	if e.cfg.Dedup {
		base += fmt.Sprintf(" QUALIFY row_number() OVER (PARTITION BY %s ORDER BY %s) = 1", seq, seq)
	}
	return fmt.Sprintf("CREATE OR REPLACE TEMP VIEW %s AS %s", e.cfg.View, base)
}
