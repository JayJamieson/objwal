package duckdbq

import (
	"context"
	"fmt"
	"sync"
)

// Result is one materialized, connection-independent query result delivered by
// Stream. Rows is row-major typed storage scanned directly into a pooled
// backing slice (no per-row interface boxing on the destination side). It is
// safe to hold and process at any pace: the producing connection was already
// released before the Result was sent, so a slow consumer cannot stall the
// stream or the engine.
//
// Call Recycle once done to return the backing slice to the stream's pool so the
// next trigger reuses it; after Recycle, do not touch Rows.
type Result[T any] struct {
	High    uint64   // window upper bound for this trigger (the new watermark)
	Columns []string // result column names, in SELECT order
	Rows    []T      // materialized rows

	recycle func()
}

// Recycle returns the backing slice to the pool. Optional; skipping it just
// means the next trigger allocates a fresh slice.
func (r *Result[T]) Recycle() {
	if r.recycle != nil {
		r.recycle()
		r.recycle = nil
		r.Rows = nil
	}
}

// StreamConfig configures a continuous query stream.
type StreamConfig[T any] struct {
	// Query is the fixed query (SQL + mode). For Incremental, the per-trigger
	// lower bound is the watermark; for Cumulative it is Query.Start.
	Query Query
	// Notify wakes the stream; each receive triggers a re-query against the
	// current visibleHigh. Closing it ends the stream.
	Notify <-chan uint64
	// Watermark persists Incremental progress; the stream advances it after each
	// emit. Ignored for Cumulative. Required for Incremental.
	Watermark WatermarkStore
	// Bind writes len(dest) field pointers of row into dest, in SELECT-column
	// order, so rows scan directly into typed fields. Required.
	Bind func(row *T, dest []any)
	// Buffer sizes the result channel (backpressure). Default 1.
	Buffer int
}

// Stream runs Config.Query continuously: on each Notify tick it resolves the
// window against the current visibleHigh, materializes the rows into a pooled
// typed slice, releases the connection, and sends a Result. The stream owns
// watermark advancement (Incremental). It ends when Notify is closed or ctx is
// cancelled, closing both returned channels.
//
// One Stream owns the engine's connection for its query lifetime; do not run a
// second Stream or concurrent one-shot Query against the same Engine. For
// multiple concurrent streams, use one Engine each.
func Stream[T any](ctx context.Context, e *Engine, cfg StreamConfig[T]) (<-chan Result[T], <-chan error, error) {
	if cfg.Bind == nil {
		return nil, nil, fmt.Errorf("duckdbq: StreamConfig.Bind is required")
	}
	if cfg.Notify == nil {
		return nil, nil, fmt.Errorf("duckdbq: StreamConfig.Notify is required")
	}
	if cfg.Query.SQL == "" {
		return nil, nil, fmt.Errorf("duckdbq: StreamConfig.Query.SQL is required")
	}
	if cfg.Query.Mode == Incremental && cfg.Watermark == nil {
		return nil, nil, fmt.Errorf("duckdbq: Incremental stream requires a Watermark")
	}
	buf := cfg.Buffer
	if buf <= 0 {
		buf = 1
	}

	out := make(chan Result[T], buf)
	errc := make(chan error, 1)
	pool := &sync.Pool{New: func() any { return []T(nil) }}

	go func() {
		defer close(out)
		defer close(errc)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-cfg.Notify:
				if !ok {
					return
				}
				if err := streamTrigger(ctx, e, cfg, pool, out); err != nil {
					if ctx.Err() != nil {
						return
					}
					errc <- err
					return
				}
			}
		}
	}()
	return out, errc, nil
}

// streamTrigger runs one re-query and (on new data) emits a Result. Returns nil
// when there is nothing new to emit.
func streamTrigger[T any](ctx context.Context, e *Engine, cfg StreamConfig[T], pool *sync.Pool, out chan<- Result[T]) error {
	hi, have, err := e.VisibleHigh(ctx)
	if err != nil {
		return err
	}
	if !have {
		return nil
	}

	var prev uint64
	if cfg.Query.Mode == Incremental {
		v, _, err := cfg.Watermark.Load(ctx)
		if err != nil {
			return err
		}
		prev = v
		// prev is the next-to-read sequence (half-open). Nothing new once it has
		// passed the visible high. A fresh prev==0 always proceeds, so seq 0 is
		// read rather than skipped.
		if prev > hi {
			return nil // no new records
		}
	}

	loIncl, hasLo, bucketLo := resolveWindow(cfg.Query, prev, e.cfg.BucketSize)
	if _, err := e.db.ExecContext(ctx, e.buildViewSQL(loIncl, hasLo, hi, bucketLo)); err != nil {
		return fmt.Errorf("duckdbq: stream build view: %w", err)
	}
	rows, err := e.db.QueryContext(ctx, cfg.Query.SQL)
	if err != nil {
		return fmt.Errorf("duckdbq: stream query: %w", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}

	out0, _ := pool.Get().([]T)
	buf := out0[:0]
	dest := make([]any, len(cols))
	for rows.Next() {
		var zero T
		buf = append(buf, zero)
		cfg.Bind(&buf[len(buf)-1], dest)
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			pool.Put(buf[:0])
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		pool.Put(buf[:0])
		return err
	}
	// Release the connection before delivering, so the consumer's pace cannot
	// hold it (the decoupling that makes a slow consumer harmless).
	rows.Close()

	// The stream owns watermark advancement (Incremental): commit before
	// delivering. The watermark is next-to-read, so it advances to hi+1. This is
	// at-most-once to the consumer across a crash; an at-least-once variant would
	// advance after the consumer acks via Recycle.
	if cfg.Query.Mode == Incremental {
		if err := cfg.Watermark.SaveCAS(ctx, prev, hi+1); err != nil {
			pool.Put(buf[:0])
			return err
		}
	}

	res := Result[T]{
		High:    hi,
		Columns: cols,
		Rows:    buf,
		recycle: func() { pool.Put(buf[:0]) },
	}
	select {
	case out <- res:
		return nil
	case <-ctx.Done():
		pool.Put(buf[:0])
		return ctx.Err()
	}
}
