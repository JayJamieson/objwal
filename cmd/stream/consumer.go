package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JayJamieson/objwal/querystream"
	"github.com/JayJamieson/objwal/querystream/duckdbq"
	"github.com/JayJamieson/objwal/querystream/parquetenc"
	"github.com/JayJamieson/objwal/wal"
)

// runConsumer tails the WAL into local Parquet via the querystream sink and, on
// each poll-interval tick, re-runs the CLI SQL through duckdbq and renders the
// result in place. In incremental mode the query watermark (next-to-read)
// advances each tick; in cumulative mode the window is [start, visibleHigh].
func runConsumer(ctx context.Context, c *Config) error {
	if c.Reset {
		if err := os.RemoveAll(c.OutDir); err != nil {
			return fmt.Errorf("reset out-dir: %w", err)
		}
		if err := os.Remove(c.CursorPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reset cursor: %w", err)
		}
	}

	store, err := newStore(ctx, c)
	if err != nil {
		return err
	}

	enc := parquetenc.New[Review](parquetenc.Options{Compression: c.Compression})
	sink, err := querystream.NewSink[Review](querystream.Config{
		ObjectStore:  store,
		ManifestPath: c.ManifestPath,
		Dir:          c.OutDir,
		BucketSize:   c.BucketSize,
		MaxRows:      c.MaxRows,
		Cursor:       wal.NewFileCursorStore(c.CursorPath),
		// In follow mode, pace ingestion so the view visibly builds across ticks
		// from a pre-loaded WAL. One-shot drains fully (see below), ignoring this.
		MaxRecordsPerPoll: c.IngestPerTick,
	}, decodeReview, enc)
	if err != nil {
		return fmt.Errorf("new sink: %w", err)
	}

	eng, err := duckdbq.Open(duckdbq.Config{
		ReadGlob:   filepath.Join(c.OutDir, "seq_bucket=*", "*.parquet"),
		SeqColumn:  "seq",
		BucketSize: c.BucketSize,
		Dedup:      c.Dedup,
	})
	if err != nil {
		return fmt.Errorf("open engine: %w", err)
	}
	defer eng.Close()

	q := duckdbq.Query{SQL: c.SQL, Start: c.StartSeq, Mode: duckdbq.Cumulative}
	if c.Incremental {
		q.Mode = duckdbq.Incremental
	}

	r := newRenderer(c)
	var wm uint64 // next-to-read watermark (incremental only)

	// emit runs the query against the current watermark, renders, and (incremental)
	// advances the half-open watermark past the high it just consumed.
	emit := func() {
		rows, newHigh, qerr := eng.Query(ctx, q, wm)
		switch {
		case errors.Is(qerr, duckdbq.ErrNoData):
			r.renderWaiting(wm)
		case qerr != nil:
			if ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "consumer: query: %v\n", qerr)
			}
		default:
			lo := wm
			if !c.Incremental {
				lo = c.StartSeq
			}
			if err := r.render(rows, lo, newHigh); err != nil {
				fmt.Fprintf(os.Stderr, "consumer: render: %v\n", err)
			}
			if c.Incremental {
				wm = newHigh + 1
			}
		}
	}

	if !c.Follow {
		// One-shot: drain the whole WAL, then render a single result.
		for {
			n, err := sink.Poll(ctx)
			if err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "consumer: poll: %v\n", err)
			}
			if err != nil || n == 0 {
				break
			}
		}
		emit()
		return nil
	}

	ticker := time.NewTicker(c.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := sink.Poll(ctx); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "consumer: poll: %v\n", err)
		}
		emit()

		select {
		case <-ctx.Done():
			fmt.Println()
			return nil
		case <-ticker.C:
		}
	}
}
