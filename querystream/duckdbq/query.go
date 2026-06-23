package duckdbq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Query resolves the [lo,hi] sequence window for q.Mode, defines the bounded
// view, and runs q.SQL against it. It returns the result rows and hi (the
// current visibleHigh, i.e. the highest sequence in the window).
//
// wm is the Incremental watermark: the next record sequence to read (half-open,
// inclusive lower bound), matching the replica cursor convention. A wm of 0
// means "nothing read yet" and reads from the first record. It is ignored for
// Cumulative, which uses q.Start as its inclusive lower bound. To resume an
// Incremental read after consuming this batch, pass hi+1 as the next wm. Returns
// ErrNoData when the read location holds no finalized files.
func (e *Engine) Query(ctx context.Context, q Query, wm uint64) (*sql.Rows, uint64, error) {
	hi, have, err := e.VisibleHigh(ctx)
	if err != nil {
		return nil, 0, err
	}
	if !have {
		return nil, 0, ErrNoData
	}

	loIncl, hasLo, bucketLo := resolveWindow(q, wm, e.cfg.BucketSize)
	view := e.buildViewSQL(loIncl, hasLo, hi, bucketLo)
	if _, err := e.db.ExecContext(ctx, view); err != nil {
		return nil, 0, fmt.Errorf("duckdbq: build view: %w", err)
	}
	rows, err := e.db.QueryContext(ctx, q.SQL)
	if err != nil {
		return nil, 0, fmt.Errorf("duckdbq: query: %w", err)
	}
	return rows, hi, nil
}

// resolveWindow maps (mode, watermark, Start) to an inclusive lower bound and
// the seq_bucket lower bound for partition pruning. Both modes take an inclusive
// lower bound: the Incremental watermark (next-to-read) or Cumulative q.Start. A
// zero lower bound means "from the beginning" and yields no predicate at all - a
// full scan with no bucket bound, so seq 0 is read and flat layouts also work.
func resolveWindow(q Query, wm uint64, bucketSize uint64) (loIncl uint64, hasLo bool, bucketLo uint64) {
	lo := wm
	if q.Mode == Cumulative {
		lo = q.Start
	}
	if lo == 0 {
		return 0, false, 0
	}
	return lo, true, lo / bucketSize
}

// buildViewSQL renders the CREATE OR REPLACE TEMP VIEW statement that bounds the
// read to [loIncl, hi] (or [.., hi] when !hasLo), injects the seq_bucket lower
// bound for file-level pruning, and optionally dedups by the seq column.
func (e *Engine) buildViewSQL(loIncl uint64, hasLo bool, hi, bucketLo uint64) string {
	seq := e.cfg.SeqColumn
	var where strings.Builder
	fmt.Fprintf(&where, "%s <= %d", seq, hi)
	if hasLo {
		fmt.Fprintf(&where, " AND %s >= %d", seq, loIncl)
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
