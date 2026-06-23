package wal

import (
	"context"
	"fmt"
)

// Record is one framed record delivered to an Applier.
type Record struct {
	// Sequence is the manifest sequence of the segment this record came from.
	// All records sharing a segment share its sequence; the cursor advances at
	// segment granularity.
	Sequence uint64
	// GroupMeta is the metadata payload of the Append group this record
	// belonged to (may be nil).
	GroupMeta []byte
	// Data is the opaque framed record bytes. The wal package never decodes
	// these; the frame format is the application's own.
	Data []byte
}

// Applier is the engine-specific path in the replication path. The replica
// hands it each framed record in sequence order, and it applies the change to
// the target system: a Bitcask applier appends the record to its active file
// and updates its keydir; a SQLite applier replays a frame; a cache applier
// updates an in-memory map. The wal package depends only on this interface and
// never on any particular engine.
//
// Apply MUST be idempotent. Produce is at-least-once, and the tailer re-applies
// a whole segment if an earlier record in it failed or if the replica restarts
// before persisting its cursor, so the same record may arrive more than once.
// For a key-value store, idempotent put/delete satisfies this with no extra
// state.
type Applier interface {
	Apply(ctx context.Context, rec Record) error
}

// ApplyFunc adapts a plain function to an Applier.
type ApplyFunc func(ctx context.Context, rec Record) error

// Apply implements Applier.
func (f ApplyFunc) Apply(ctx context.Context, rec Record) error { return f(ctx, rec) }

// DecodeFunc parses an opaque record's bytes into a typed operation. You own
// this: it mirrors however the writer framed the record.
type DecodeFunc[T any] func(data []byte) (T, error)

// HandleFunc applies a decoded operation to the target system, e.g. by calling
// into the Bitcask (or other) engine's API. You own this too.
type HandleFunc[T any] func(ctx context.Context, seq uint64, op T) error

// TypedApplier is the recommended way to plug an engine in: supply a decoder
// that turns record bytes into your operation type and a handler that applies
// it. The wal layer owns the boilerplate (pull the record, decode, dispatch)
// and stays ignorant of both the frame format and the engine - neither the
// decode nor the apply logic lives in this package.
//
//	applier := wal.TypedApplier(
//		decodeKVOp,                                  // []byte -> KVOp (yours)
//		func(ctx context.Context, seq uint64, op KVOp) error {
//			switch op.Kind {
//			case OpPut:    return db.Put(op.Key, op.Value)  // db is yours
//			case OpDelete: return db.Delete(op.Key)
//			}
//			return nil
//		},
//	)
//
// Decode/handle errors surface from Poll; because Apply must be idempotent, a
// failure simply causes the segment to be re-delivered on the next pass.
func TypedApplier[T any](decode DecodeFunc[T], handle HandleFunc[T]) Applier {
	return ApplyFunc(func(ctx context.Context, rec Record) error {
		op, err := decode(rec.Data)
		if err != nil {
			return fmt.Errorf("wal: decode record at seq %d: %w", rec.Sequence, err)
		}
		return handle(ctx, rec.Sequence, op)
	})
}
