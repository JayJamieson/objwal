// Command s3bench measures end-to-end WAL write and read throughput against an
// S3-backed objectstore, reported in megabits per second.
//
// Unlike a raw objectstore PUT/GET microbenchmark, this drives the actual
// replication path: writes go through the epoch-fenced wal.Producer (group
// commit, segment upload, manifest CAS) and reads go through wal.Replica
// (manifest tail, segment fetch+decode, Applier dispatch). The numbers are
// therefore what the WAL itself sustains on top of S3, not the floor underneath
// it. Two subcommands:
//
//	s3bench write [flags]   # Append -count records of -size bytes through the
//	                        # producer; report durable-write Mbps.
//	s3bench read  [flags]   # Tail the manifest with a replica, applying every
//	                        # record; report apply Mbps.
//
// write and read must agree on -manifest and -prefix so the replica tails what
// the producer wrote. Connection settings come from the same environment
// variables scripts/test.sh exports (S3_ENDPOINT, S3_BUCKET, AWS_REGION,
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY); any can be overridden with a flag.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "write":
		err = runWrite(os.Args[2:])
	case "read":
		err = runRead(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `s3bench measures end-to-end WAL throughput over S3 in megabits per second.

usage:
  s3bench write [flags]   Append -count records of -size bytes via wal.Producer, report write Mbps
  s3bench read  [flags]   Tail the manifest via wal.Replica and apply every record, report read Mbps

run "s3bench write -h" or "s3bench read -h" for flags.
`)
}

// conn holds the connection and log-location flags shared by both subcommands.
// manifest and prefix must match between a write and the read that tails it.
type conn struct {
	endpoint string
	bucket   string
	region   string
	prefix   string
	manifest string
}

func (c *conn) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.endpoint, "endpoint", os.Getenv("S3_ENDPOINT"), "S3 endpoint URL (default $S3_ENDPOINT)")
	fs.StringVar(&c.bucket, "bucket", envOr("S3_BUCKET", "s3bench"), "bucket name (default $S3_BUCKET)")
	fs.StringVar(&c.region, "region", envOr("AWS_REGION", "us-east-1"), "AWS region (default $AWS_REGION)")
	fs.StringVar(&c.prefix, "prefix", "s3bench/seg", "key prefix for WAL segment objects")
	fs.StringVar(&c.manifest, "manifest", "s3bench/manifest", "manifest object key")
}

func runWrite(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	var c conn
	c.bind(fs)
	count := fs.Int("count", 256, "number of records to append")
	size := fs.Int("size", 1<<20, "record size in bytes")
	concurrency := fs.Int("concurrency", 16, "concurrent in-flight Append calls")
	flushBytes := fs.Int("flush-bytes", 8<<20, "seal a segment once buffered record bytes reach this (0 = interval only)")
	flushInterval := fs.Duration("flush-interval", 50*time.Millisecond, "max time records wait before a segment is sealed")
	segMaxBytes := fs.Int("segment-max-bytes", 16<<20, "cap on a single segment object size (0 = one segment per flush)")
	uploadConc := fs.Int("upload-concurrency", 4, "concurrent segment uploads within a flush")
	manifestBatch := fs.Int("manifest-batch", 0, "segment entries coalesced per manifest CAS (0 = all of a flush)")
	latency := fs.Bool("latency", false, "measure per-record durable-commit latency (Append→Wait) and report percentiles instead of aggregate throughput")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	store, err := newStore(ctx, c)
	if err != nil {
		return err
	}

	producer, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:            c.manifest,
		SegmentPrefix:           c.prefix,
		FlushInterval:           *flushInterval,
		FlushBytes:              *flushBytes,
		SegmentMaxBytes:         *segMaxBytes,
		UploadConcurrency:       *uploadConc,
		ManifestAppendBatchSize: *manifestBatch,
	})
	if err != nil {
		return fmt.Errorf("claim log: %w", err)
	}

	// One random payload reused across appends. The producer does not copy or
	// mutate records, and reads are concurrent-safe, so reuse only saves the
	// generation cost; it does not change the bytes uploaded.
	payload := make([]byte, *size)
	if _, err := rand.Read(payload); err != nil {
		return err
	}

	fmt.Printf("write: %d records x %s = %s, concurrency=%d, manifest=%q, prefix=%q\n",
		*count, humanBytes(int64(*size)), humanBytes(int64(*count)*int64(*size)), *concurrency, c.manifest, c.prefix)
	fmt.Printf("  epoch=%d flush-bytes=%s flush-interval=%s segment-max=%s upload-concurrency=%d\n",
		producer.Epoch(), humanBytes(int64(*flushBytes)), *flushInterval, humanBytes(int64(*segMaxBytes)), *uploadConc)

	if *latency {
		return runWriteLatency(ctx, producer, payload, *count, *concurrency)
	}

	// Append from a worker pool so several groups are in flight; each worker
	// records the Durability handle so we can wait for the whole batch to commit
	// before stopping the clock. Throughput is therefore measured end-to-end:
	// admission + segment upload + manifest commit.
	durs := make([]*wal.Durability, *count)
	var written int64
	start := time.Now()
	err = parallel(*count, *concurrency, func(i int) error {
		d, err := producer.Append(ctx, [][]byte{payload}, nil)
		if err != nil {
			return err
		}
		durs[i] = d
		atomic.AddInt64(&written, int64(len(payload)))
		return nil
	})
	if err != nil {
		_ = producer.Close(ctx)
		return fmt.Errorf("append: %w", err)
	}
	for i, d := range durs {
		if _, err := d.Wait(ctx); err != nil {
			_ = producer.Close(ctx)
			return fmt.Errorf("durability wait (record %d): %w", i, err)
		}
	}
	elapsed := time.Since(start)

	if err := producer.Close(ctx); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}

	report("write", written, *count, elapsed)
	return nil
}

// runWriteLatency measures per-record durable-commit latency. Each worker times
// a single Append→Wait round trip — the real time from submission to manifest
// commit — so up to `concurrency` records are in flight at once. Unlike the
// throughput path (which fires every Append then waits the batch), this reflects
// what a caller actually observes per write, including the FlushInterval the
// record waits to be sealed. Drive it with small flush thresholds and low
// concurrency (e.g. -flush-bytes 0 -flush-interval 1ms -concurrency 1) to see
// the floor.
func runWriteLatency(ctx context.Context, producer *wal.Producer, payload []byte, count, concurrency int) error {
	lat := make([]time.Duration, count)
	var written int64
	start := time.Now()
	err := parallel(count, concurrency, func(i int) error {
		t0 := time.Now()
		d, err := producer.Append(ctx, [][]byte{payload}, nil)
		if err != nil {
			return err
		}
		if _, err := d.Wait(ctx); err != nil {
			return err
		}
		lat[i] = time.Since(t0)
		atomic.AddInt64(&written, int64(len(payload)))
		return nil
	})
	wall := time.Since(start)
	if err != nil {
		_ = producer.Close(ctx)
		return fmt.Errorf("append: %w", err)
	}
	if err := producer.Close(ctx); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}

	reportLatency("write commit", lat, written, wall, concurrency)
	return nil
}

// reportLatency prints the latency distribution of per-record commits alongside
// the aggregate throughput observed over the same run.
func reportLatency(label string, lat []time.Duration, bytes int64, wall time.Duration, concurrency int) {
	n := len(lat)
	if n == 0 {
		fmt.Printf("%s: no samples\n", label)
		return
	}
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		idx := int(p / 100 * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return sorted[idx]
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(n)
	mbps := float64(bytes) * 8 / 1e6 / wall.Seconds()

	fmt.Printf("%s latency (n=%d, %d in flight):\n", label, n, concurrency)
	fmt.Printf("  min=%s  p50=%s  p90=%s  p99=%s  max=%s  mean=%s\n",
		rd(sorted[0]), rd(pct(50)), rd(pct(90)), rd(pct(99)), rd(sorted[n-1]), rd(mean))
	fmt.Printf("  aggregate: %.1f Mbps over %s\n", mbps, wall.Round(time.Millisecond))
}

// rd rounds a duration to a readable unit for display.
func rd(d time.Duration) time.Duration {
	switch {
	case d < time.Microsecond:
		return d
	case d < time.Millisecond:
		return d.Round(time.Microsecond)
	default:
		return d.Round(100 * time.Microsecond)
	}
}

func runRead(args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	var c conn
	c.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	store, err := newStore(ctx, c)
	if err != nil {
		return err
	}

	var read int64
	var records int64
	applier := wal.ApplyFunc(func(ctx context.Context, rec wal.Record) error {
		atomic.AddInt64(&read, int64(len(rec.Data)))
		atomic.AddInt64(&records, 1)
		return nil
	})

	replica := wal.NewReplica(store, applier, wal.ReplicaConfig{
		ManifestPath: c.manifest,
		StartAt:      0,
	})

	fmt.Printf("read: tailing manifest=%q prefix=%q\n", c.manifest, c.prefix)

	// Tail until a poll applies nothing: the producer wrote the whole log in a
	// prior invocation, so the manifest is complete and the first empty pass
	// means we are caught up.
	start := time.Now()
	for {
		n, err := replica.Poll(ctx)
		if err != nil {
			return fmt.Errorf("poll: %w", err)
		}
		if n == 0 {
			break
		}
	}
	elapsed := time.Since(start)

	if records == 0 {
		return fmt.Errorf("no records under manifest %q — run \"s3bench write\" first", c.manifest)
	}

	report("read", read, int(records), elapsed)
	return nil
}

// report prints throughput in Mbps (megabits/sec, base-10) alongside MB/s and
// records/sec, the three numbers a storage benchmark is usually read against.
func report(label string, bytes int64, records int, elapsed time.Duration) {
	secs := elapsed.Seconds()
	mbps := float64(bytes) * 8 / 1e6 / secs
	mbytes := float64(bytes) / 1e6 / secs
	ops := float64(records) / secs
	fmt.Printf("%s: %s in %s\n", label, humanBytes(bytes), elapsed.Round(time.Millisecond))
	fmt.Printf("  %.1f Mbps  (%.1f MB/s, %.0f rec/s)\n", mbps, mbytes, ops)
}

// parallel runs fn(0..n-1) across at most `concurrency` workers and returns the
// first error encountered (remaining items still drain so workers exit cleanly).
func parallel(n, concurrency int, fn func(i int) error) error {
	if concurrency < 1 {
		concurrency = 1
	}
	work := make(chan int, concurrency)
	var (
		wg       sync.WaitGroup
		firstErr error
		errMu    sync.Mutex
	)
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				if err := fn(i); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	return firstErr
}

// newStore builds the S3-backed objectstore from the connection flags and the
// AWS credential env vars, then ensures the bucket exists.
func newStore(ctx context.Context, c conn) (objectstore.ObjectStore, error) {
	cfg, _ := config.LoadDefaultConfig(context.Background(), config.WithRegion(c.region))
	client := s3.NewFromConfig(cfg)

	if err := ensureBucket(ctx, client, c.bucket); err != nil {
		return nil, err
	}
	return objectstore.NewS3(client, c.bucket), nil
}

// ensureBucket creates the bucket if it does not already exist. A bucket the
// caller already owns is not an error.
func ensureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	if err == nil {
		return nil
	}
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
