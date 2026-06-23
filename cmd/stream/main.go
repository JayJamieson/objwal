// Command stream is an end-to-end demo of the objwal query-streaming stack. In
// producer mode it loads a CSV into the epoch-fenced WAL (stored in MinIO/S3);
// in consumer mode it tails the WAL through querystream.Sink into local Parquet
// and runs a CLI-supplied SQL query with DuckDB, re-rendering the result in
// place on each refresh.
//
//	CSV ─producer─▶ WAL ─▶ MinIO ─consumer:Sink─▶ Parquet ─duckdbq─▶ SQL ─▶ console
//
// It is a separate (nested) module so the CGO/DuckDB dependency stays out of the
// pure-Go WAL/core build graph; build with CGO_ENABLED=1.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "stream:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Mode {
	case "producer":
		err = runProducer(ctx, cfg)
	case "consumer":
		err = runConsumer(ctx, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "stream:", err)
		os.Exit(1)
	}
}
