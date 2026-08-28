package querystream

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JayJamieson/objwal/wal"
)

// Sink tails a WAL and materializes it into seq-partitioned files.
type Sink[T any] struct {
	cfg     Config
	decode  Decoder[T]
	enc     Encoder[T]
	replica *wal.Replica

	// Owned by the single ingest goroutine (Poll/Run); not mutex-protected.
	buf         []rowItem[T]
	approxBytes int

	// Read by VisibleHigh/Catalog concurrently; guarded by mu.
	mu          sync.RWMutex
	visibleHigh uint64
	haveData    bool
	catalog     []FileInfo

	nextCursor uint64 // next sequence to read; == lastFlushed+1

	// notify is a coalescing wakeup: flushOne signals the new visibleHigh after
	// each watermark advance. Single-slot, drop-on-full, so ingest never blocks
	// on an absent or slow consumer. See Notify.
	notify chan uint64
}

type rowItem[T any] struct {
	seq uint64
	row T
	sz  int
}

// NewSink builds a sink. It resolves the start sequence (persisted cursor, else
// Config.StartAt), seeds visibleHigh/catalog from any files already in Dir, and
// wires a wal.Replica that delivers records to the sink's batching applier.
func NewSink[T any](cfg Config, decode Decoder[T], enc Encoder[T]) (*Sink[T], error) {
	if cfg.ObjectStore == nil {
		return nil, fmt.Errorf("querystream: ObjectStore is required")
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("querystream: Dir is required")
	}
	if decode == nil || enc == nil {
		return nil, fmt.Errorf("querystream: Decoder and Encoder are required")
	}
	cfg.withDefaults()
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("querystream: create dir: %w", err)
	}

	s := &Sink[T]{cfg: cfg, decode: decode, enc: enc, notify: make(chan uint64, 1)}

	// Resume point: persisted cursor wins over StartAt.
	start := cfg.StartAt
	if cfg.Cursor != nil {
		if v, ok, err := cfg.Cursor.Load(context.Background()); err != nil {
			return nil, fmt.Errorf("querystream: load cursor: %w", err)
		} else if ok {
			start = v
		}
	}
	s.nextCursor = start

	if err := s.scanExisting(); err != nil {
		return nil, err
	}

	s.replica = wal.NewReplica(cfg.ObjectStore, wal.ApplyFunc(s.onRecord), wal.ReplicaConfig{
		ManifestPath:      cfg.ManifestPath,
		StartAt:           start,
		MaxRecordsPerPoll: cfg.MaxRecordsPerPoll,
		// No CursorStore on the replica: the sink advances the cursor itself,
		// only after rows are durable in a finalized file.
	})
	return s, nil
}

// Poll performs one ingest cycle: drain newly-committed WAL records into the
// batch (flushing at bucket boundaries and size caps), then finalize whatever
// remains so nothing is left buffered. Returns the number of records applied.
// Must be called from a single goroutine.
func (s *Sink[T]) Poll(ctx context.Context) (int, error) {
	n, err := s.replica.Poll(ctx)
	if err != nil {
		// Drop the partial batch; the replica will re-deliver from the last
		// un-advanced segment on the next attempt (at-least-once).
		s.buf = s.buf[:0]
		s.approxBytes = 0
		return n, err
	}
	for {
		flushed, ferr := s.flushOne(ctx)
		if ferr != nil {
			return n, ferr
		}
		if !flushed {
			break
		}
	}
	return n, nil
}

// Run drives Poll in a loop until ctx is cancelled.
func (s *Sink[T]) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		if _, err := s.Poll(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// onRecord is the wal.Applier: decode, buffer, and flush on a bucket boundary
// or a size cap. The buffer only ever holds rows from a single bucket.
func (s *Sink[T]) onRecord(ctx context.Context, rec wal.Record) error {
	row, err := s.decode(rec)
	if err != nil {
		return fmt.Errorf("querystream: decode seq %d: %w", rec.Sequence, err)
	}
	if len(s.buf) > 0 && s.bucketOf(rec.Sequence) != s.bucketOf(s.buf[0].seq) {
		if _, err := s.flushOne(ctx); err != nil {
			return err
		}
	}
	s.buf = append(s.buf, rowItem[T]{seq: rec.Sequence, row: row, sz: len(rec.Data)})
	s.approxBytes += len(rec.Data)
	if len(s.buf) >= s.cfg.MaxRows || (s.cfg.MaxBytes > 0 && s.approxBytes >= s.cfg.MaxBytes) {
		if _, err := s.flushOne(ctx); err != nil {
			return err
		}
	}
	return nil
}

// flushOne finalizes the leading single-bucket run of the buffer into one file.
// Because onRecord flushes at bucket boundaries, the buffer is always a single
// bucket, so this finalizes the whole buffer. Returns false if nothing buffered.
func (s *Sink[T]) flushOne(ctx context.Context) (bool, error) {
	if len(s.buf) == 0 {
		return false, nil
	}
	bucket := s.bucketOf(s.buf[0].seq)
	end := 1
	for end < len(s.buf) && s.bucketOf(s.buf[end].seq) == bucket {
		end++
	}
	run := s.buf[:end]
	first, last := run[0].seq, run[end-1].seq
	seqs := make([]uint64, end)
	rows := make([]T, end)
	for i := range run {
		seqs[i] = run[i].seq
		rows[i] = run[i].row
	}

	path := s.filePath(bucket, first, last)
	if err := s.writeAtomic(path, seqs, rows); err != nil {
		return false, fmt.Errorf("querystream: write %s: %w", path, err)
	}

	// Commit: advance buffer, catalog, watermark.
	s.buf = s.buf[end:]
	s.approxBytes = 0
	for _, it := range s.buf {
		s.approxBytes += it.sz
	}
	info := FileInfo{Path: path, Bucket: bucket, FirstSeq: first, LastSeq: last, Rows: end}
	s.mu.Lock()
	s.catalog = append(s.catalog, info)
	s.visibleHigh = last
	s.haveData = true
	s.mu.Unlock()
	s.nextCursor = last + 1

	// Coalescing, non-blocking wakeup: a full slot already carries "there's new
	// data", and the value is only a hint (consumers read VisibleHigh for truth).
	select {
	case s.notify <- last:
	default:
	}

	if s.cfg.Cursor != nil {
		if err := s.cfg.Cursor.Save(ctx, s.nextCursor); err != nil {
			return false, fmt.Errorf("querystream: save cursor: %w", err)
		}
	}
	return true, nil
}

func (s *Sink[T]) writeAtomic(path string, seqs []uint64, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := s.enc.Encode(tmp, seqs, rows); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Sink[T]) bucketOf(seq uint64) uint64 { return seq / s.cfg.BucketSize }

func (s *Sink[T]) filePath(bucket, first, last uint64) string {
	return filepath.Join(
		s.cfg.Dir,
		fmt.Sprintf("seq_bucket=%020d", bucket),
		fmt.Sprintf("part-%020d-%020d%s", first, last, s.enc.Ext()),
	)
}

// VisibleHigh returns the highest record sequence durably present in a finalized
// file, and whether any data is visible yet. Queries should read seq <= high.
func (s *Sink[T]) VisibleHigh() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.visibleHigh, s.haveData
}

// Notify returns a coalescing channel that receives the new visibleHigh each
// time a flush advances the watermark. It is single-slot and drop-on-full: a
// slow or absent consumer never blocks ingest, and a missed (coalesced) tick is
// harmless because the value is only a wakeup - read VisibleHigh for the truth.
// Pass it to duckdbq.StreamConfig.Notify to drive a continuous in-process query
// loop. The channel is not closed; stop the consuming Stream by cancelling its
// context.
func (s *Sink[T]) Notify() <-chan uint64 { return s.notify }

// Catalog returns a snapshot of the finalized files and their sequence ranges.
func (s *Sink[T]) Catalog() []FileInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]FileInfo(nil), s.catalog...)
}

// scanExisting rebuilds the catalog and watermark from files already in Dir, so
// a restarted process knows what is visible without re-ingesting.
func (s *Sink[T]) scanExisting() error {
	ext := s.enc.Ext()
	pattern := filepath.Join(s.cfg.Dir, "seq_bucket=*", "part-*-*"+ext)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, m := range matches {
		first, last, ok := parsePartName(filepath.Base(m), ext)
		if !ok {
			continue
		}
		bucket, _ := parseBucketDir(filepath.Base(filepath.Dir(m)))
		s.catalog = append(s.catalog, FileInfo{Path: m, Bucket: bucket, FirstSeq: first, LastSeq: last})
		if !s.haveData || last > s.visibleHigh {
			s.visibleHigh = last
			s.haveData = true
		}
	}
	return nil
}

func parsePartName(base, ext string) (first, last uint64, ok bool) {
	if !strings.HasPrefix(base, "part-") || !strings.HasSuffix(base, ext) {
		return 0, 0, false
	}
	mid := strings.TrimSuffix(strings.TrimPrefix(base, "part-"), ext)
	dash := strings.IndexByte(mid, '-')
	if dash < 0 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseUint(mid[:dash], 10, 64)
	last, err2 := strconv.ParseUint(mid[dash+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return first, last, true
}

func parseBucketDir(dir string) (uint64, bool) {
	const p = "seq_bucket="
	if !strings.HasPrefix(dir, p) {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimPrefix(dir, p), 10, 64)
	return v, err == nil
}

// SegmentStartSeq resolves a segment index (0-based position in the manifest's
// live entries) to the record sequence its segment begins at, so a caller can
// start ingest "from segment N" instead of a raw sequence.
func SegmentStartSeq(ctx context.Context, store *wal.Store, segIndex int) (uint64, error) {
	m, _, ok, err := store.Load(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("querystream: no manifest at path")
	}
	entries, err := m.Entries()
	if err != nil {
		return 0, err
	}
	if segIndex < 0 || segIndex >= len(entries) {
		return 0, fmt.Errorf("querystream: segment index %d out of range [0,%d)", segIndex, len(entries))
	}
	return entries[segIndex].Sequence, nil
}
