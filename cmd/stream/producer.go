package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/JayJamieson/objwal/wal"
)

// runProducer streams the CSV into the WAL: batch-size rows per Append, paced by
// append-delay, optionally looping forever. Each looped pass re-appends the rows
// with fresh, distinct sequences.
func runProducer(ctx context.Context, c *Config) error {
	store, err := newStore(ctx, c)
	if err != nil {
		return err
	}
	rows, err := readReviews(c.CSV)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("no rows in %s", c.CSV)
	}

	comp := wal.CompressionNone
	if c.Compression == "zstd" {
		comp = wal.CompressionZstd
	}
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:    c.ManifestPath,
		SegmentPrefix:   c.SegmentPrefix,
		FlushInterval:   c.FlushInterval,
		SegmentMaxBytes: c.SegmentMaxBytes,
		Compression:     comp,
	})
	if err != nil {
		return fmt.Errorf("new producer: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cerr := p.Close(closeCtx); cerr != nil {
			fmt.Fprintf(os.Stderr, "producer: close: %v\n", cerr)
		}
	}()

	fmt.Printf("producer: %d rows, batch=%d, delay=%s, loop=%v\n", len(rows), c.BatchSize, c.AppendDelay, c.Loop)
	for {
		if err := streamPass(ctx, p, rows, c); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if !c.Loop {
			return nil
		}
	}
}

// streamPass appends every row once, in batch-size chunks.
func streamPass(ctx context.Context, p *wal.Producer, rows []Review, c *Config) error {
	for i := 0; i < len(rows); i += c.BatchSize {
		if ctx.Err() != nil {
			return context.Canceled
		}
		end := min(i+c.BatchSize, len(rows))
		batch := make([][]byte, 0, end-i)
		for _, r := range rows[i:end] {
			b, err := encodeReview(r)
			if err != nil {
				return fmt.Errorf("encode row %d: %w", r.Index, err)
			}
			batch = append(batch, b)
		}
		d, err := p.Append(ctx, batch, nil)
		if err != nil {
			return fmt.Errorf("append: %w", err)
		}
		rng, err := d.WaitRange(ctx)
		if err != nil {
			return fmt.Errorf("durability wait: %w", err)
		}
		logAppend(rows[i:end], rng)
		if c.AppendDelay > 0 {
			select {
			case <-ctx.Done():
				return context.Canceled
			case <-time.After(c.AppendDelay):
			}
		}
	}
	return nil
}

func logAppend(batch []Review, rng wal.SeqRange) {
	if len(batch) == 1 {
		r := batch[0]
		fmt.Printf("producer: appended 1 row   seq=%d  product=%q rating=%d\n", rng.First, r.ProductName, r.Rating)
		return
	}
	fmt.Printf("producer: appended %d rows  seq=[%d..%d]\n", len(batch), rng.First, rng.Last())
}

// readReviews parses the CSV (header row skipped) into Review rows. Seq is left
// zero; the WAL assigns it on append. Column order:
// Index,Review ID,Product Name,Rating,Review Text.
func readReviews(path string) ([]Review, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	rd := csv.NewReader(f)
	rd.FieldsPerRecord = -1
	var out []Review
	first := true
	for {
		rec, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read csv: %w", err)
		}
		if first { // skip header
			first = false
			continue
		}
		if len(rec) < 5 {
			continue
		}
		idx, _ := strconv.ParseInt(trim(rec[0]), 10, 64)
		rating, _ := strconv.ParseInt(trim(rec[3]), 10, 64)
		out = append(out, Review{
			Index:       idx,
			ReviewID:    trim(rec[1]),
			ProductName: trim(rec[2]),
			Rating:      rating,
			ReviewText:  trim(rec[4]),
		})
	}
	return out, nil
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
