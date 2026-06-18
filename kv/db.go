// Package kv is a thin, bitcask-style key-value store built on the objwal
// write-ahead-log primitives. An in-memory keydir plus a local append-only file
// back low-latency reads, while objwal owns durability, ordering, and
// replication.
//
// Open returns a read-write primary backed by a wal.Producer; OpenReplica
// returns a read-only replica backed by a wal.Replica. Both share the same
// local read core, so a key reads identically on the primary and every replica.
//
// Concurrency: concurrent writes to DIFFERENT keys are fully safe and parallel.
// Concurrent writes to the SAME key from multiple goroutines are NOT ordered by
// this package: the keydir is updated after each write's objwal commit resolves,
// in goroutine-completion order, which may differ from objwal sequence order. So
// the surviving value — on the primary itself, and relative to replicas — is
// whichever write completed last, not necessarily the one objwal ordered last.
// Do not write the same key concurrently from multiple goroutines.
package kv

import (
	"context"
	"errors"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// Config configures a primary (read-write) DB.
type Config struct {
	// ManifestPath is the objwal manifest object key.
	ManifestPath string
	// SegmentPrefix is the objwal segment key prefix.
	SegmentPrefix string
	// LocalPath is the path to the local append-only data file.
	LocalPath string
	// FlushInterval bounds how long a write waits before objwal seals a segment
	// (forwarded to wal.ProducerConfig; 0 uses the producer default).
	FlushInterval time.Duration
	// FlushBytes seals a segment early once buffered bytes reach it (forwarded
	// to wal.ProducerConfig; 0 disables size-based flushing).
	FlushBytes int
}

// DB is a read-write key-value store: the single objwal primary plus a local
// bitcask-style read cache.
type DB struct {
	local *local
	prod  *wal.Producer
}

// Open constructs a primary DB. It recovers the keydir from the local file if
// present, otherwise replays objwal from the beginning, then claims the objwal
// log as the primary (bumping the epoch). A wal.ErrFenced error means a newer
// primary already owns the log.
func Open(ctx context.Context, store objectstore.ObjectStore, cfg Config) (*DB, error) {
	l, err := openLocal(cfg.LocalPath)
	if err != nil {
		return nil, err
	}
	// Recovery: trust the local file when it has data; otherwise rebuild from
	// objwal. A single Poll pass applies every committed entry currently in the
	// manifest. This assumes the single-writer model: no other primary is
	// committing during cold replay (the producer constructed below fences
	// others only once it exists).
	if l.isEmpty() {
		rep := wal.NewReplica(store, localApplier(l), wal.ReplicaConfig{ManifestPath: cfg.ManifestPath})
		if _, err := rep.Poll(ctx); err != nil {
			l.close()
			return nil, err
		}
	}
	prod, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:  cfg.ManifestPath,
		SegmentPrefix: cfg.SegmentPrefix,
		FlushInterval: cfg.FlushInterval,
		FlushBytes:    cfg.FlushBytes,
	})
	if err != nil {
		l.close()
		return nil, err
	}
	return &DB{local: l, prod: prod}, nil
}

// Put durably stores value under key. It returns only after the write is
// committed to objwal (durably replicated); the value is in the local keydir by
// the time it returns. The objwal commit happens without holding the local lock,
// so concurrent Put/Delete coalesce into one group commit.
func (db *DB) Put(ctx context.Context, key, value []byte) error {
	return db.write(ctx, opPut, key, value)
}

// Delete removes key. Like Put, it returns only after the deletion is committed
// to objwal.
func (db *DB) Delete(ctx context.Context, key []byte) error {
	return db.write(ctx, opDelete, key, nil)
}

func (db *DB) write(ctx context.Context, op byte, key, value []byte) error {
	d, err := db.prod.Append(ctx, [][]byte{encodeOp(op, key, value)}, nil)
	if err != nil {
		return err
	}
	if _, err := d.Wait(ctx); err != nil {
		return err
	}
	return db.local.apply(op, key, value)
}

// Get returns the value for key. found is false when the key is absent.
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	return db.local.get(key)
}

// Close flushes and halts the objwal producer and closes the local file. It
// must be called exactly once.
func (db *DB) Close(ctx context.Context) error {
	perr := db.prod.Close(ctx)
	lerr := db.local.close()
	return errors.Join(perr, lerr)
}

// localApplier adapts the shared local.apply path to a wal.Applier, used both by
// the primary's objwal-replay recovery and by replica tailing. Apply is
// idempotent: re-applying a put/delete is safe.
func localApplier(l *local) wal.Applier {
	return wal.ApplyFunc(func(ctx context.Context, rec wal.Record) error {
		op, key, value, err := decodeOp(rec.Data)
		if err != nil {
			return err
		}
		return l.apply(op, key, value)
	})
}
