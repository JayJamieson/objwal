// Package parquetenc provides a Parquet implementation of
// querystream.Encoder[T], writing seq-sorted Parquet files that DuckDB reads
// directly (locally or over s3:// via httpfs).
//
// It is an isolated module (own go.mod) so the parquet-go dependency stays out
// of the WAL/core graph. Build it in a Go >= 1.24 toolchain.
//
// Contract: your row type T must be a struct whose exported fields carry
// `parquet:"..."` tags, and it must include the record sequence as a column
// named "seq" (the sink fills it from wal.Record.Sequence in your Decoder).
// That seq column is what gives DuckDB automatic row-group pruning for
// `WHERE seq >= X`, and it is the primary key the query layer dedups on.
//
// Usage:
//
//	type Row struct {
//	    Seq uint64 `parquet:"seq"`
//	    Key string `parquet:"key"`
//	    Val string `parquet:"val"`
//	}
//	enc := parquetenc.New[Row](parquetenc.Options{Compression: "zstd"})
//	sink, _ := querystream.NewSink[Row](cfg, decode, enc)
package parquetenc

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// Options configures the Parquet writer.
type Options struct {
	// Compression is one of "", "snappy" (default), or "zstd".
	Compression string
	// SeqColumn is the column the writer sorts and ranges on (default "seq").
	SeqColumn string
	// RowGroupTargetRows caps rows per row group (0 = parquet-go default).
	RowGroupTargetRows int64
}

// Encoder is the querystream.Encoder[T] implementation. T must satisfy the
// struct/tag contract described in the package doc.
type Encoder[T any] struct {
	opts  Options
	codec compress.Codec
	seq   string
}

// New builds a Parquet encoder for row type T.
func New[T any](opts Options) *Encoder[T] {
	seq := opts.SeqColumn
	if seq == "" {
		seq = "seq"
	}
	var codec compress.Codec
	switch opts.Compression {
	case "", "snappy":
		codec = &snappy.Codec{}
	case "zstd":
		codec = &zstd.Codec{}
	default:
		codec = &snappy.Codec{}
	}
	return &Encoder[T]{opts: opts, codec: codec, seq: seq}
}

// Ext implements querystream.Encoder.
func (e *Encoder[T]) Ext() string { return ".parquet" }

// Encode implements querystream.Encoder: it writes rows (already ascending by
// sequence) to a single Parquet file, sorted by the seq column and carrying
// row-group statistics so a reader can prune by sequence range. seqs is
// accepted for interface symmetry; the seq value is expected to live in each
// row's seq-tagged field, which the sink populated.
func (e *Encoder[T]) Encode(path string, seqs []uint64, rows []T) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	opts := []parquet.WriterOption{
		parquet.Compression(e.codec),
		parquet.SortingWriterConfig(
			parquet.SortingColumns(parquet.Ascending(e.seq)),
		),
	}
	if e.opts.RowGroupTargetRows > 0 {
		opts = append(opts, parquet.MaxRowsPerRowGroup(e.opts.RowGroupTargetRows))
	}

	w := parquet.NewGenericWriter[T](f, opts...)
	if _, err := w.Write(rows); err != nil {
		w.Close()
		return fmt.Errorf("parquetenc: write %d rows: %w", len(rows), err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("parquetenc: close: %w", err)
	}
	return nil
}
