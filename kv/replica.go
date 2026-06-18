package kv

import (
	"context"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// ReplicaConfig configures a read-only replica.
type ReplicaConfig struct {
	// ManifestPath is the objwal manifest object key (must match the primary).
	ManifestPath string
	// LocalPath is the path to this replica's local append-only data file.
	LocalPath string
	// CursorPath persists the tail position so a restart resumes instead of
	// replaying from the beginning. Defaults to LocalPath + ".cursor".
	CursorPath string
	// PollInterval is how often Run polls objwal (forwarded to wal.ReplicaConfig;
	// 0 uses the replica default).
	PollInterval time.Duration
}

// Replica is a read-only key-value view that tails objwal and serves local
// reads. Many replicas may tail the same log concurrently; readers are not
// epoch-fenced.
type Replica struct {
	local *local
	rep   *wal.Replica
}

// OpenReplica constructs a read-only replica. It recovers the keydir from the
// local file if present, then tails objwal from the persisted cursor (or from
// the beginning if there is none). Call Poll to step once or Run to tail
// continuously.
func OpenReplica(ctx context.Context, store objectstore.ObjectStore, cfg ReplicaConfig) (*Replica, error) {
	l, err := openLocal(cfg.LocalPath)
	if err != nil {
		return nil, err
	}
	cursorPath := cfg.CursorPath
	if cursorPath == "" {
		cursorPath = cfg.LocalPath + ".cursor"
	}
	rep := wal.NewReplica(store, localApplier(l), wal.ReplicaConfig{
		ManifestPath: cfg.ManifestPath,
		PollInterval: cfg.PollInterval,
		Cursor:       wal.NewFileCursorStore(cursorPath),
	})
	return &Replica{local: l, rep: rep}, nil
}

// Get returns the value for key. found is false when the key is absent.
func (r *Replica) Get(key []byte) (value []byte, found bool, err error) {
	return r.local.get(key)
}

// Poll runs one tail pass, applying every objwal entry past the cursor and
// returning the number of records applied.
func (r *Replica) Poll(ctx context.Context) (int, error) {
	return r.rep.Poll(ctx)
}

// Run tails objwal until ctx is cancelled, returning ctx.Err() on exit.
func (r *Replica) Run(ctx context.Context) error {
	return r.rep.Run(ctx)
}

// Close closes the local file.
func (r *Replica) Close() error {
	return r.local.close()
}
