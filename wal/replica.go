package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
)

// ReplicaConfig configures the read replica's tail loop.
type ReplicaConfig struct {
	// ManifestPath is the manifest object key (must match the producer).
	ManifestPath string
	// PollInterval is how often Run polls the manifest for new entries.
	PollInterval time.Duration
	// StartAt is the initial cursor: the next sequence to apply. The replica
	// applies entries with Sequence >= StartAt. Without snapshots it defaults
	// to 0 (replay from the beginning); bootstrapping from a snapshot would
	// set it to the snapshot's ThroughSeq + 1.
	StartAt uint64
	// Cursor, if set, persists the next-to-apply cursor so a restart resumes
	// instead of replaying from StartAt. A persisted value overrides StartAt.
	Cursor CursorStore
	// CursorSaveInterval batches cursor saves: the cursor is persisted at most
	// once per this many applied segments, plus once at the end of each poll
	// pass that applied anything. 0 or 1 saves after every segment (safest;
	// default). Larger values trade a few extra re-applied segments after a
	// crash (idempotent) for fewer fsyncs.
	CursorSaveInterval int
	// MaxRecordsPerPoll bounds how many records a single Poll applies: once a
	// poll has applied at least this many, it stops at the next segment boundary
	// (cursor saved there) and returns, leaving the rest for the following Poll.
	// 0 means unbounded (drain everything available). Granularity is a whole
	// segment, so the effective cap is "the first segment boundary at or past
	// this count". Useful for pacing ingestion so progress is observable.
	MaxRecordsPerPoll int
}

func (c *ReplicaConfig) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 50 * time.Millisecond
	}
	if c.CursorSaveInterval <= 0 {
		c.CursorSaveInterval = 1
	}
}

// Replica tails the WAL and applies records to a local state machine via an
// Applier. Many replicas may tail the same manifest concurrently; readers are
// not epoch-fenced.
type Replica struct {
	store    *Store
	os       objectstore.ObjectStore
	apply    Applier
	cfg      ReplicaConfig
	next     uint64
	cursor   CursorStore
	restored bool
}

// NewReplica constructs a replica positioned at cfg.StartAfter.
func NewReplica(os objectstore.ObjectStore, apply Applier, cfg ReplicaConfig) *Replica {
	cfg.withDefaults()
	return &Replica{
		store:  NewStore(os, cfg.ManifestPath),
		os:     os,
		apply:  apply,
		cfg:    cfg,
		next:   cfg.StartAt,
		cursor: cfg.Cursor,
	}
}

// Next returns the next sequence the replica expects to apply (one past the
// highest applied entry).
func (r *Replica) Next() uint64 { return r.next }

// Poll runs one tail pass: it loads the manifest, fetches and applies every
// segment with Sequence > cursor in order, and advances the cursor (per segment). It returns
// the number of records applied. The cursor advances past a segment only after
// all of its records have been applied, so a mid-segment failure re-applies the
// whole segment next time (hence the idempotency requirement).
func (r *Replica) Poll(ctx context.Context) (int, error) {
	if r.cursor != nil && !r.restored {
		next, ok, err := r.cursor.Load(ctx)
		if err != nil {
			return 0, err
		}
		if ok {
			r.next = next
		}
		r.restored = true
	}
	m, _, ok, err := r.store.Load(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil // manifest not created yet
	}
	entries, err := m.EntriesContaining(r.next)
	if err != nil {
		return 0, err
	}
	applied := 0
	sinceSave := 0
	resumeAt := r.next // records below this were already applied (mid-segment resume)
	saveCursor := func() error {
		if r.cursor == nil {
			return nil
		}
		if err := r.cursor.Save(ctx, r.next); err != nil {
			return fmt.Errorf("wal: persist cursor at next %d: %w", r.next, err)
		}
		sinceSave = 0
		return nil
	}
	for _, e := range entries {
		res, err := r.os.Get(ctx, e.Location)
		if err != nil {
			return applied, fmt.Errorf("wal: replica get %s: %w", e.Location, err)
		}
		records, err := decodeSegment(res.Data)
		if err != nil {
			return applied, fmt.Errorf("wal: replica decode %s: %w", e.Location, err)
		}
		perRecord := e.Count > 0
		for i, data := range records {
			recSeq := e.Sequence
			if perRecord {
				recSeq = e.Sequence + uint64(i)
			}
			if recSeq < resumeAt {
				continue // already applied; resuming mid-segment from resumeAt
			}
			rec := Record{Sequence: recSeq, GroupMeta: groupMetaFor(e.Metadata, i), Data: data}
			if err := r.apply.Apply(ctx, rec); err != nil {
				return applied, fmt.Errorf("wal: apply seq %d: %w", recSeq, err)
			}
			applied++
		}
		r.next = e.End()
		sinceSave++
		if sinceSave >= r.cfg.CursorSaveInterval {
			if err := saveCursor(); err != nil {
				return applied, err
			}
		}
		// Pace ingestion: stop at this segment boundary once the per-poll budget
		// is met. The trailing saveCursor below persists r.next = e.End().
		if r.cfg.MaxRecordsPerPoll > 0 && applied >= r.cfg.MaxRecordsPerPoll {
			break
		}
	}
	// Always persist the final position at the end of a poll that advanced.
	if sinceSave > 0 {
		if err := saveCursor(); err != nil {
			return applied, err
		}
	}
	return applied, nil
}

// Run polls until ctx is cancelled, returning ctx.Err() on exit. Apply or
// fetch errors are returned immediately (the caller decides whether to retry).
func (r *Replica) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.Poll(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
		}
	}
}

// groupMetaFor returns the metadata payload of the Append group covering the
// record at index i within a segment. Metadata ranges are ordered by
// StartIndex; the covering range is the last one whose StartIndex <= i.
func groupMetaFor(metas []RecordMeta, i int) []byte {
	var payload []byte
	for _, md := range metas {
		if int(md.StartIndex) <= i {
			payload = md.Payload
		} else {
			break
		}
	}
	return payload
}
