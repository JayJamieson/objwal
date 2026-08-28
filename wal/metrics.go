package wal

import (
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName identifies this package's instruments to a MeterProvider.
const instrumentationName = "github.com/JayJamieson/objwal/wal"

type producerMetrics struct {
	appendRecords  metric.Int64Counter
	appendBytes    metric.Int64Counter
	batchRecords   metric.Int64Histogram
	batchBytes     metric.Int64Histogram
	commitDuration metric.Float64Histogram
	commits        metric.Int64Counter // attr outcome=ok|error
	commitRetries  metric.Int64Counter // attr reason=conflict|unknown|load
	claims         metric.Int64Counter // attr outcome=ok|error
	claimRetries   metric.Int64Counter
	fenced         metric.Int64Counter
	halted         metric.Int64Counter
	uploadFailures metric.Int64Counter
}

func newProducerMetrics(log *slog.Logger, meter metric.Meter) *producerMetrics {
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(instrumentationName)
	}
	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	pm := &producerMetrics{}
	var err error
	pm.appendRecords, err = meter.Int64Counter("objwal.wal.append.records",
		metric.WithDescription("Records passed to Append"), metric.WithUnit("{record}"))
	check(err)
	pm.appendBytes, err = meter.Int64Counter("objwal.wal.append.bytes",
		metric.WithDescription("Record bytes passed to Append"), metric.WithUnit("By"))
	check(err)
	pm.batchRecords, err = meter.Int64Histogram("objwal.wal.batch.records",
		metric.WithDescription("Records per committed segment"), metric.WithUnit("{record}"))
	check(err)
	pm.batchBytes, err = meter.Int64Histogram("objwal.wal.batch.bytes",
		metric.WithDescription("Bytes per committed segment"), metric.WithUnit("By"))
	check(err)
	pm.commitDuration, err = meter.Float64Histogram("objwal.wal.commit.duration",
		metric.WithDescription("Manifest commit latency"), metric.WithUnit("s"))
	check(err)
	pm.commits, err = meter.Int64Counter("objwal.wal.commit.count",
		metric.WithDescription("Manifest commit attempts by outcome"), metric.WithUnit("{commit}"))
	check(err)
	pm.commitRetries, err = meter.Int64Counter("objwal.wal.commit.retries",
		metric.WithDescription("Manifest commit retries by reason"), metric.WithUnit("{retry}"))
	check(err)
	pm.claims, err = meter.Int64Counter("objwal.wal.claim.count",
		metric.WithDescription("Epoch claim attempts by outcome"), metric.WithUnit("{claim}"))
	check(err)
	pm.claimRetries, err = meter.Int64Counter("objwal.wal.claim.retries",
		metric.WithDescription("Epoch claim retries"), metric.WithUnit("{retry}"))
	check(err)
	pm.fenced, err = meter.Int64Counter("objwal.wal.fenced",
		metric.WithDescription("Times this producer observed a newer epoch"), metric.WithUnit("{event}"))
	check(err)
	pm.halted, err = meter.Int64Counter("objwal.wal.halted",
		metric.WithDescription("Times this producer transitioned to halted"), metric.WithUnit("{event}"))
	check(err)
	pm.uploadFailures, err = meter.Int64Counter("objwal.wal.upload.failures",
		metric.WithDescription("Segment uploads that exhausted retries and halted the producer"), metric.WithUnit("{failure}"))
	check(err)

	if len(errs) > 0 {
		log.Warn("metrics: some instruments failed to register", "count", len(errs), "first_err", errs[0])
	}
	return pm
}

type replicaMetrics struct {
	applied      metric.Int64Counter
	pollDuration metric.Float64Histogram
}

func newReplicaMetrics(log *slog.Logger, meter metric.Meter) *replicaMetrics {
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(instrumentationName)
	}
	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	rm := &replicaMetrics{}
	var err error
	rm.applied, err = meter.Int64Counter("objwal.wal.replica.applied",
		metric.WithDescription("Records applied by this replica"), metric.WithUnit("{record}"))
	check(err)
	rm.pollDuration, err = meter.Float64Histogram("objwal.wal.replica.poll.duration",
		metric.WithDescription("Poll call latency"), metric.WithUnit("s"))
	check(err)

	if len(errs) > 0 {
		log.Warn("metrics: some instruments failed to register", "count", len(errs), "first_err", errs[0])
	}
	return rm
}

var (
	attrOutcomeOK    = attribute.String("outcome", "ok")
	attrOutcomeError = attribute.String("outcome", "error")

	// commit retry reasons.
	attrReasonLoad          = attribute.String("reason", "load")           // Load() itself failed
	attrReasonCASConflict   = attribute.String("reason", "cas_conflict")   // 412: definitely lost the race
	attrReasonWriteConflict = attribute.String("reason", "write_conflict") // 409: concurrent write in flight
	attrReasonUnknown       = attribute.String("reason", "unknown")        // outcome unproven either way
)
