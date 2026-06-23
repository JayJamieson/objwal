package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config is the fully-resolved demo configuration: JSON file defaults overlaid
// by any explicitly-set flags. Durations are already resolved from the JSON
// *_ms fields or their flag overrides.
type Config struct {
	Mode string // "producer" or "consumer"

	// MinIO / S3
	Endpoint, AccessKey, SecretKey, Region, Bucket string
	PathStyle, CreateBucket                        bool

	// WAL object keys (shared by producer & consumer)
	ManifestPath, SegmentPrefix string

	// Producer
	CSV             string
	BatchSize       int
	AppendDelay     time.Duration
	Loop            bool
	FlushInterval   time.Duration
	SegmentMaxBytes int

	// Consumer: sink
	OutDir, CursorPath string
	BucketSize         uint64
	MaxRows            int
	Compression        string
	PollInterval       time.Duration
	Reset              bool

	// Consumer: query
	SQL           string
	Incremental   bool
	StartSeq      uint64
	Dedup         bool
	NoClear       bool
	Follow        bool // loop, refreshing on each poll-interval (else one-shot)
	IngestPerTick int  // cap WAL records ingested per tick (0 = drain all)
}

// jsonConfig mirrors demo.json (ms-based durations) for unmarshaling.
type jsonConfig struct {
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	PathStyle    *bool  `json:"path_style"`
	CreateBucket *bool  `json:"create_bucket"`

	ManifestPath  string `json:"manifest_path"`
	SegmentPrefix string `json:"segment_prefix"`

	FlushIntervalMs int   `json:"flush_interval_ms"`
	SegmentMaxBytes int   `json:"segment_max_bytes"`
	BatchSize       int   `json:"batch_size"`
	AppendDelayMs   int   `json:"append_delay_ms"`
	Loop            *bool `json:"loop"`

	OutDir         string `json:"out_dir"`
	CursorPath     string `json:"cursor_path"`
	BucketSize     uint64 `json:"bucket_size"`
	MaxRows        int    `json:"max_rows"`
	Compression    string `json:"compression"`
	PollIntervalMs int    `json:"poll_interval_ms"`

	Dedup         *bool `json:"dedup"`
	NoClear       *bool `json:"no_clear"`
	Follow        *bool `json:"follow"`
	IngestPerTick int   `json:"ingest_per_tick"`
}

// defaultConfig returns the built-in defaults (used when no -config and as the
// base the JSON file overlays).
func defaultConfig() *Config {
	return &Config{
		Endpoint:        "http://localhost:9000",
		AccessKey:       "minioadmin",
		SecretKey:       "minioadmin",
		Region:          "us-east-1",
		Bucket:          "objwal-demo",
		PathStyle:       true,
		CreateBucket:    true,
		ManifestPath:    "wal/manifest",
		SegmentPrefix:   "wal/segments",
		BatchSize:       1,
		AppendDelay:     500 * time.Millisecond,
		FlushInterval:   200 * time.Millisecond,
		SegmentMaxBytes: 65536,
		OutDir:          "./_demo_data",
		BucketSize:      1000,
		MaxRows:         2000,
		Compression:     "zstd",
		PollInterval:    time.Second,
		Dedup:           true,
		Follow:          true,
	}
}

// loadConfig resolves configuration from args: it reads an optional -config JSON
// file into the defaults, then registers flags seeded from that result so any
// explicitly-set flag overrides the JSON value.
func loadConfig(args []string) (*Config, error) {
	c := defaultConfig()

	// Pre-scan for -config so its values seed the real flag defaults.
	if path := scanConfigFlag(args); path != "" {
		if err := applyJSON(c, path); err != nil {
			return nil, err
		}
	}

	fs := flag.NewFlagSet("stream", flag.ContinueOnError)
	fs.StringVar(&c.Mode, "mode", c.Mode, "producer or consumer (required)")
	fs.String("config", "", "path to JSON config; flags override its values")

	fs.StringVar(&c.Endpoint, "endpoint", c.Endpoint, "MinIO/S3 endpoint")
	fs.StringVar(&c.AccessKey, "access-key", c.AccessKey, "S3 access key")
	fs.StringVar(&c.SecretKey, "secret-key", c.SecretKey, "S3 secret key")
	fs.StringVar(&c.Region, "region", c.Region, "S3 region")
	fs.StringVar(&c.Bucket, "bucket", c.Bucket, "S3 bucket")
	fs.BoolVar(&c.PathStyle, "path-style", c.PathStyle, "path-style addressing (MinIO)")
	fs.BoolVar(&c.CreateBucket, "create-bucket", c.CreateBucket, "create bucket if missing")
	fs.StringVar(&c.ManifestPath, "manifest", c.ManifestPath, "manifest object key")
	fs.StringVar(&c.SegmentPrefix, "segment-prefix", c.SegmentPrefix, "segment key prefix")

	// Producer
	fs.StringVar(&c.CSV, "csv", c.CSV, "input CSV path (producer)")
	fs.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "CSV rows per WAL Append")
	fs.DurationVar(&c.AppendDelay, "append-delay", c.AppendDelay, "pacing between Appends")
	fs.BoolVar(&c.Loop, "loop", c.Loop, "replay CSV forever")
	fs.DurationVar(&c.FlushInterval, "flush-interval", c.FlushInterval, "WAL FlushInterval")
	fs.IntVar(&c.SegmentMaxBytes, "segment-max-bytes", c.SegmentMaxBytes, "WAL SegmentMaxBytes")

	// Consumer
	fs.StringVar(&c.SQL, "sql", c.SQL, "query over the records view (consumer)")
	fs.BoolVar(&c.Incremental, "incremental", c.Incremental, "new rows only vs cumulative")
	fs.Uint64Var(&c.StartSeq, "start-seq", c.StartSeq, "cumulative lower bound")
	fs.StringVar(&c.OutDir, "out-dir", c.OutDir, "local Parquet output dir")
	cursorFlag := fs.String("cursor", c.CursorPath, "ingest cursor file (default <out-dir>/cursor)")
	fs.Uint64Var(&c.BucketSize, "bucket-size", c.BucketSize, "seq partition size")
	fs.IntVar(&c.MaxRows, "max-rows", c.MaxRows, "rows per Parquet file")
	fs.StringVar(&c.Compression, "compression", c.Compression, "parquet codec")
	fs.DurationVar(&c.PollInterval, "poll-interval", c.PollInterval, "ingest+query+refresh cadence")
	fs.BoolVar(&c.Dedup, "dedup", c.Dedup, "dedup rows by seq")
	fs.BoolVar(&c.NoClear, "no-clear", c.NoClear, "append output instead of in-place refresh")
	fs.BoolVar(&c.Reset, "reset", c.Reset, "wipe out-dir + cursor before starting")
	fs.BoolVar(&c.Follow, "follow", c.Follow, "loop, refreshing each poll-interval (false = one-shot)")
	fs.IntVar(&c.IngestPerTick, "ingest-per-tick", c.IngestPerTick, "cap WAL records ingested per tick (0 = drain all); paces the live view")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c.CursorPath = *cursorFlag
	if c.CursorPath == "" {
		c.CursorPath = filepath.Join(c.OutDir, "cursor")
	}
	return c, validate(c)
}

func validate(c *Config) error {
	switch c.Mode {
	case "producer":
		if c.CSV == "" {
			return fmt.Errorf("producer mode requires -csv")
		}
	case "consumer":
		if c.SQL == "" {
			return fmt.Errorf("consumer mode requires -sql")
		}
	case "":
		return fmt.Errorf("-mode is required (producer or consumer)")
	default:
		return fmt.Errorf("invalid -mode %q (want producer or consumer)", c.Mode)
	}
	return nil
}

// scanConfigFlag returns the value of -config / --config from args, or "".
func scanConfigFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-config" || a == "--config" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		for _, p := range []string{"-config=", "--config="} {
			if len(a) > len(p) && a[:len(p)] == p {
				return a[len(p):]
			}
		}
	}
	return ""
}

// applyJSON overlays a demo.json file onto c, resolving ms durations.
func applyJSON(c *Config, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	var j jsonConfig
	if err := json.Unmarshal(b, &j); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	setStr(&c.Endpoint, j.Endpoint)
	setStr(&c.AccessKey, j.AccessKey)
	setStr(&c.SecretKey, j.SecretKey)
	setStr(&c.Region, j.Region)
	setStr(&c.Bucket, j.Bucket)
	setBool(&c.PathStyle, j.PathStyle)
	setBool(&c.CreateBucket, j.CreateBucket)
	setStr(&c.ManifestPath, j.ManifestPath)
	setStr(&c.SegmentPrefix, j.SegmentPrefix)
	setDurMs(&c.FlushInterval, j.FlushIntervalMs)
	setInt(&c.SegmentMaxBytes, j.SegmentMaxBytes)
	setInt(&c.BatchSize, j.BatchSize)
	setDurMs(&c.AppendDelay, j.AppendDelayMs)
	setBool(&c.Loop, j.Loop)
	setStr(&c.OutDir, j.OutDir)
	setStr(&c.CursorPath, j.CursorPath)
	setU64(&c.BucketSize, j.BucketSize)
	setInt(&c.MaxRows, j.MaxRows)
	setStr(&c.Compression, j.Compression)
	setDurMs(&c.PollInterval, j.PollIntervalMs)
	setBool(&c.Dedup, j.Dedup)
	setBool(&c.NoClear, j.NoClear)
	setBool(&c.Follow, j.Follow)
	setInt(&c.IngestPerTick, j.IngestPerTick)
	return nil
}

func setStr(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}
func setInt(dst *int, v int) {
	if v != 0 {
		*dst = v
	}
}
func setU64(dst *uint64, v uint64) {
	if v != 0 {
		*dst = v
	}
}
func setBool(dst *bool, v *bool) {
	if v != nil {
		*dst = *v
	}
}
func setDurMs(dst *time.Duration, ms int) {
	if ms != 0 {
		*dst = time.Duration(ms) * time.Millisecond
	}
}
