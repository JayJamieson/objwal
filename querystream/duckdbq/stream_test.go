package duckdbq

import (
	"context"
	"testing"
)

type srow struct {
	Seq uint64
	Val string
}

// bindSrow points dest at srow's fields in SELECT order (seq, val). No
// reflection: the consumer knows its own fixed query.
func bindSrow(r *srow, dest []any) {
	dest[0] = &r.Seq
	dest[1] = &r.Val
}

func TestStreamIncrementalAdvancesWatermark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	writeFixtures(t, dir, 10, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	e := openLocal(t, dir, 10)
	wm := NewMemWatermarkStore()

	notify := make(chan uint64, 4)
	out, errc, err := Stream[srow](ctx, e, StreamConfig[srow]{
		Query:     Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Incremental},
		Notify:    notify,
		Watermark: wm,
		Bind:      bindSrow,
		Buffer:    2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Trigger 1: from wm=0 exclusive => seq 1..9.
	notify <- 9
	res := <-out
	if res.High != 9 {
		t.Fatalf("res.High = %d, want 9", res.High)
	}
	if len(res.Rows) != 9 || res.Rows[0].Seq != 1 || res.Rows[8].Seq != 9 {
		t.Fatalf("trigger1 rows = %d [%d..], want 9 starting at 1", len(res.Rows), res.Rows[0].Seq)
	}
	if res.Rows[0].Val != "v1" {
		t.Fatalf("trigger1 val = %q, want v1", res.Rows[0].Val)
	}
	// Stream owns advancement: the watermark is committed by the time we receive.
	if v, _, _ := wm.Load(ctx); v != 9 {
		t.Fatalf("watermark = %d, want 9", v)
	}

	// Decoupling proof: the result is materialized (no live *sql.Rows), so the
	// engine's single connection is free even while we still hold res unrecycled.
	// With a live-rows result under MaxOpenConns(1) this one-shot query would
	// block forever and time the test out.
	rows, _, err := e.Query(ctx, Query{SQL: "SELECT count(*) AS c FROM records", Mode: Cumulative}, 0)
	if err != nil {
		t.Fatalf("one-shot query while holding res: %v", err)
	}
	rows.Close()
	res.Recycle()

	// Trigger 2: append more, from wm=9 => seq 10..14.
	writeFixtures(t, dir, 10, []uint64{10, 11, 12, 13, 14})
	notify <- 14
	res2 := <-out
	if res2.High != 14 || len(res2.Rows) != 5 || res2.Rows[0].Seq != 10 || res2.Rows[4].Seq != 14 {
		t.Fatalf("trigger2 rows = %d high %d [%d..], want 5 high 14 starting 10", len(res2.Rows), res2.High, res2.Rows[0].Seq)
	}
	if v, _, _ := wm.Load(ctx); v != 14 {
		t.Fatalf("watermark after trigger2 = %d, want 14", v)
	}
	res2.Recycle()

	// A trigger with no new data does not emit and does not advance.
	notify <- 14
	notify <- 14 // a second tick we can observe arriving to know the first was handled
	// drain nothing: closing notify ends the stream; if an empty emit had been
	// produced it would sit in `out` and the range below would see it.
	close(notify)

	var extra int
	for range out {
		extra++
	}
	if extra != 0 {
		t.Fatalf("got %d unexpected emits for no-new-data triggers, want 0", extra)
	}

	cancel()
	if err := <-errc; err != nil && err != context.Canceled {
		t.Fatalf("stream error: %v", err)
	}
}

func TestStreamCumulativeRescans(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	writeFixtures(t, dir, 10, []uint64{0, 1, 2})
	e := openLocal(t, dir, 10)

	notify := make(chan uint64, 4)
	out, _, err := Stream[srow](ctx, e, StreamConfig[srow]{
		Query:  Query{SQL: "SELECT seq, val FROM records ORDER BY seq", Mode: Cumulative, Start: 0},
		Notify: notify,
		Bind:   bindSrow,
		Buffer: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	notify <- 2
	r1 := <-out
	if len(r1.Rows) != 3 {
		t.Fatalf("cumulative trigger1 = %d rows, want 3", len(r1.Rows))
	}
	r1.Recycle()

	// Cumulative re-scans from Start each trigger: after appending, it returns
	// the full set again, not just the delta.
	writeFixtures(t, dir, 10, []uint64{3, 4})
	notify <- 4
	r2 := <-out
	if len(r2.Rows) != 5 || r2.Rows[0].Seq != 0 || r2.Rows[4].Seq != 4 {
		t.Fatalf("cumulative trigger2 = %d rows [%d..%d], want 5 [0..4]", len(r2.Rows), r2.Rows[0].Seq, r2.Rows[len(r2.Rows)-1].Seq)
	}
	r2.Recycle()
}
