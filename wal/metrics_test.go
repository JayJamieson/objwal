package wal_test

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// counterTotal sums every data point of an int64 sum instrument by name, or
// -1 if the instrument was never recorded.
func counterTotal(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s is not an int64 Sum", name)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return -1
}

func histogramCount(t *testing.T, rm metricdata.ResourceMetrics, name string) uint64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch h := m.Data.(type) {
			case metricdata.Histogram[int64]:
				var n uint64
				for _, dp := range h.DataPoints {
					n += dp.Count
				}
				return n
			case metricdata.Histogram[float64]:
				var n uint64
				for _, dp := range h.DataPoints {
					n += dp.Count
				}
				return n
			}
		}
	}
	return 0
}

func TestMetrics_AppendCommitBatch(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	os := objectstore.NewInMemory()
	p, err := wal.NewProducer(ctx, os, wal.ProducerConfig{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/seg",
		FlushInterval: 3 * time.Millisecond,
		Meter:         mp.Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := p.Append(ctx, [][]byte{[]byte("a"), []byte("b")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}

	if got := counterTotal(t, rm, "objwal.wal.append.records"); got != 2 {
		t.Fatalf("append.records = %d, want 2", got)
	}
	if got := counterTotal(t, rm, "objwal.wal.claim.count"); got != 1 {
		t.Fatalf("claim.count = %d, want 1 (one successful claim)", got)
	}
	if got := counterTotal(t, rm, "objwal.wal.commit.count"); got < 1 {
		t.Fatalf("commit.count = %d, want >= 1", got)
	}
	if got := histogramCount(t, rm, "objwal.wal.batch.records"); got < 1 {
		t.Fatalf("batch.records histogram count = %d, want >= 1", got)
	}
	if got := histogramCount(t, rm, "objwal.wal.commit.duration"); got < 1 {
		t.Fatalf("commit.duration histogram count = %d, want >= 1", got)
	}
	// A clean Close halts the producer but that's not a fencing/error event.
	// An instrument with zero Add() calls reports no data points at all, so
	// "never recorded" (-1, the counterTotal sentinel) is the expected value.
	if got := counterTotal(t, rm, "objwal.wal.fenced"); got > 0 {
		t.Fatalf("fenced = %d, want none recorded", got)
	}
	if got := counterTotal(t, rm, "objwal.wal.halted"); got != 1 {
		t.Fatalf("halted = %d, want 1 (the Close)", got)
	}
}

func TestMetrics_FencingIncrementsFencedAndHalted(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	os := objectstore.NewInMemory()

	cfg := wal.ProducerConfig{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/seg",
		FlushInterval: 3 * time.Millisecond,
		Meter:         mp.Meter("test"),
	}
	primaryA, err := wal.NewProducer(ctx, os, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = primaryA.Close(ctx) }()

	primaryB, err := wal.NewProducer(ctx, os, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = primaryB.Close(ctx) }()

	// A is now fenced.
	dA, _ := primaryA.Append(ctx, [][]byte{[]byte("zombie")}, nil)
	if _, err := dA.Wait(ctx); err == nil {
		t.Fatal("expected fenced write to fail")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if got := counterTotal(t, rm, "objwal.wal.fenced"); got < 1 {
		t.Fatalf("fenced = %d, want >= 1", got)
	}
	if got := counterTotal(t, rm, "objwal.wal.halted"); got < 1 {
		t.Fatalf("halted = %d, want >= 1 (fencing halts the producer)", got)
	}
}

func TestMetrics_ReplicaAppliedAndPollDuration(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	os := objectstore.NewInMemory()

	p, err := wal.NewProducer(ctx, os, wal.ProducerConfig{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/seg",
		FlushInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := p.Append(ctx, [][]byte{[]byte("x")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	_ = p.Close(ctx)

	r := wal.NewReplica(os, wal.ApplyFunc(func(context.Context, wal.Record) error { return nil }), wal.ReplicaConfig{
		ManifestPath: "wal/manifest",
		Meter:        mp.Meter("test"),
	})
	if _, err := r.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if got := counterTotal(t, rm, "objwal.wal.replica.applied"); got != 1 {
		t.Fatalf("replica.applied = %d, want 1", got)
	}
	if got := histogramCount(t, rm, "objwal.wal.replica.poll.duration"); got < 1 {
		t.Fatalf("replica.poll.duration histogram count = %d, want >= 1", got)
	}
}

func TestMetrics_NilMeterDefaultsToNoop(t *testing.T) {
	ctx := context.Background()
	os := objectstore.NewInMemory()
	// No Meter set: must resolve against the global (no-op) provider and
	// never panic on a nil instrument.
	p, err := wal.NewProducer(ctx, os, wal.ProducerConfig{
		ManifestPath:  "wal/manifest",
		SegmentPrefix: "wal/seg",
		FlushInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := p.Append(ctx, [][]byte{[]byte("x")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
