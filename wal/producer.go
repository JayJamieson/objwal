package wal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/oklog/ulid/v2"
)

// ErrFenced is returned once a newer primary has claimed the log (bumped the
// epoch). A fenced producer is permanently halted; construct a fresh one only
// after re-establishing that this node is the primary.
var ErrFenced = errors.New("wal: producer fenced by a newer epoch")

// ErrHalted is returned by Append after the producer has stopped (a fatal
// commit error, fencing, or Close).
var ErrHalted = errors.New("wal: producer halted")

// ErrNoRecords is returned by Append when given no records. An empty group
// would produce an invalid manifest entry and halt the producer.
var ErrNoRecords = errors.New("wal: Append requires at least one record")

const (
	defaultMaxInFlightBytes   = 256 << 20 // 256 MiB
	defaultMaxInFlightBatches = 4096
)

// ProducerConfig configures the primary's write path.
type ProducerConfig struct {
	// ManifestPath is the manifest object key.
	ManifestPath string
	// SegmentPrefix is the key prefix for segment objects; segments are named
	// "<SegmentPrefix>/<runID>/<ordinal:016x>".
	SegmentPrefix string
	// FlushInterval bounds how long records wait before a segment is sealed.
	FlushInterval time.Duration
	// FlushBytes seals a segment early once buffered record bytes reach it.
	// Zero disables size-based flushing.
	FlushBytes int
	// Compression applied to segment record blocks.
	Compression Compression
	// LegacySegmentFormat writes unchecksummed batch-v1 segments instead of
	// v2, for downstream consumers that read them as upstream buffer batches.
	// Gives up corruption detection. Reading v1 is always supported.
	LegacySegmentFormat bool

	// MaxInFlightBytes caps the total bytes Appended but not yet durably
	// committed. Append BLOCKS once a further record would exceed it - the
	// primary backpressure signal (default 256 MiB). A single Append larger
	// than the cap is admitted only when nothing else is in flight, to avoid
	// deadlock.
	MaxInFlightBytes int
	// MaxInFlightBatches caps the number of un-committed Append calls in
	// flight - a secondary safety stop against many tiny Appends (default
	// 4096).
	MaxInFlightBatches int

	// SegmentMaxBytes caps the size of a single segment object. When a flush
	// drains more than this, it rotates into multiple segments (whole Append
	// groups are kept intact; an oversized group gets its own segment). Zero
	// means one segment per flush (no rotation).
	SegmentMaxBytes int
	// ManifestAppendBatchSize caps how many segment entries are coalesced into
	// one manifest CAS. Zero coalesces all of a flush's segments into a single
	// CAS. Coalescing cuts manifest write rate/cost and raises the throughput
	// ceiling (bytes-per-commit), at the cost of one CAS covering several
	// segments.
	ManifestAppendBatchSize int

	// MaxClaimAttempts bounds epoch-claim CAS retries (default 8).
	MaxClaimAttempts int
	// UploadMaxAttempts bounds segment-upload retries (default 6).
	UploadMaxAttempts int
	// UploadInitialBackoff is the first upload retry backoff (default 100ms).
	UploadInitialBackoff time.Duration
	// UploadConcurrency bounds how many segment uploads run in parallel within
	// a flush (default 4). Uploads overlap, but commits remain strictly serial
	// and in ordinal order, so total log order is preserved. Set to 1 for fully
	// sequential uploads.
	UploadConcurrency int
	// ManifestMaxAttempts bounds retries of *transient* manifest-commit errors
	// (default 6). A precondition conflict is a free re-plan (not counted), and
	// the re-load's epoch check converts a competing writer into ErrFenced.
	ManifestMaxAttempts int
	// ManifestInitialBackoff is the first backoff for transient commit retries
	// (default 100ms).
	ManifestInitialBackoff time.Duration
	// MaxCommitConflicts bounds how many times a commit may lose the manifest
	// CAS race (412) before giving up (default 64), so a misclassified error
	// cannot spin forever.
	MaxCommitConflicts int
	// Logger receives structured lifecycle events: claims, fencing, commit
	// retries/conflicts, upload failures, halts. Defaults to a text logger on
	// stderr filtered to Warn, so a healthy producer is silent; pass your own
	// *slog.Logger (any handler, any level, including Debug/Info for routine
	// flow) to change verbosity or destination.
	Logger *slog.Logger
	// Meter records batch/commit/retry/fencing metrics (see producerMetrics).
	// Defaults to otel.GetMeterProvider()'s meter, which is a no-op until the
	// process calls otel.SetMeterProvider - so metrics cost nothing unless
	// wired up, here or globally.
	Meter metric.Meter
}

func (c *ProducerConfig) withDefaults() {
	if c.FlushInterval <= 0 {
		c.FlushInterval = 50 * time.Millisecond
	}
	if c.MaxInFlightBytes <= 0 {
		c.MaxInFlightBytes = defaultMaxInFlightBytes
	}
	if c.MaxInFlightBatches <= 0 {
		c.MaxInFlightBatches = defaultMaxInFlightBatches
	}
	if c.MaxClaimAttempts <= 0 {
		c.MaxClaimAttempts = 16
	}
	if c.UploadMaxAttempts <= 0 {
		c.UploadMaxAttempts = 6
	}
	if c.UploadInitialBackoff <= 0 {
		c.UploadInitialBackoff = 100 * time.Millisecond
	}
	if c.UploadConcurrency <= 0 {
		c.UploadConcurrency = 4
	}
	if c.ManifestMaxAttempts <= 0 {
		c.ManifestMaxAttempts = 6
	}
	if c.ManifestInitialBackoff <= 0 {
		c.ManifestInitialBackoff = 100 * time.Millisecond
	}
	if c.MaxCommitConflicts <= 0 {
		c.MaxCommitConflicts = 64
	}
	if c.Logger == nil {
		c.Logger = defaultLogger()
	}
}

// Durability resolves when the records of an Append have been committed to the
// manifest (or failed permanently).
type Durability struct {
	done chan struct{}
	seq  uint64
	n    int
	err  error
}

func newDurability(n int) *Durability { return &Durability{done: make(chan struct{}), n: n} }

func (d *Durability) resolve(seq uint64, err error) {
	d.seq, d.err = seq, err
	close(d.done)
}

// Wait blocks until the records are durable, returning the assigned manifest
// sequence, or ctx's error if it is cancelled first.
func (d *Durability) Wait(ctx context.Context) (uint64, error) {
	select {
	case <-d.done:
		return d.seq, d.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Count is the number of records this Append wrote. They occupy the contiguous
// record-sequence range [first, first+Count), where first is what Wait returns.
func (d *Durability) Count() int { return d.n }

// WaitRange blocks like Wait and returns the full record-sequence range this
// Append occupies, so callers never hand-roll first+i arithmetic. This is the
// preferred accessor for a multi-record Append.
func (d *Durability) WaitRange(ctx context.Context) (SeqRange, error) {
	first, err := d.Wait(ctx)
	if err != nil {
		return SeqRange{}, err
	}
	return SeqRange{First: first, Count: d.n}, nil
}

// SeqRange is the contiguous record-sequence range a single Append occupies:
// its records have sequences [First, First+Count). Use it instead of
// recomputing First+i offsets at call sites, which is an easy place to
// introduce off-by-one errors.
type SeqRange struct {
	First uint64
	Count int
}

// End is the exclusive upper bound of the range (First+Count).
func (r SeqRange) End() uint64 { return r.First + uint64(r.Count) }

// Last is the sequence of the final record (First+Count-1); only meaningful
// when Count >= 1.
func (r SeqRange) Last() uint64 { return r.First + uint64(r.Count) - 1 }

// At returns the sequence of the i-th record in the Append (0-indexed). It
// panics if i is outside [0, Count).
func (r SeqRange) At(i int) uint64 {
	if i < 0 || i >= r.Count {
		panic(fmt.Sprintf("wal: SeqRange.At(%d) out of range [0,%d)", i, r.Count))
	}
	return r.First + uint64(i)
}

// Contains reports whether seq is one of this Append's record sequences.
func (r SeqRange) Contains(seq uint64) bool { return seq >= r.First && seq < r.End() }

// All materializes every sequence in the range. Prefer First/Count/At for large
// batches; this allocates a slice.
func (r SeqRange) All() []uint64 {
	out := make([]uint64, r.Count)
	for i := range out {
		out[i] = r.First + uint64(i)
	}
	return out
}

type pendingItem struct {
	records [][]byte
	meta    []byte
	bytes   int
	w       *Durability
}

// segPlan is one segment to be uploaded and committed: the Append groups it
// carries, its encoded records, the per-group metadata, and its object name.
type segPlan struct {
	items    []pendingItem
	records  [][]byte
	metas    []RecordMeta
	bytes    int
	location string
}

// Producer is the epoch-fenced single-writer primary. It accepts opaque framed
// records, group-commits them into segment objects, and CAS-appends manifest
// entries. It knows nothing about record contents.
type Producer struct {
	store   *Store
	os      objectstore.ObjectStore
	cfg     ProducerConfig
	now     func() time.Time
	log     *slog.Logger
	metrics *producerMetrics

	epoch uint64
	runID string

	mu            sync.Mutex
	pending       []pendingItem
	pendByte      int
	inFlightBytes int
	inFlightCount int
	ordinal       uint64
	halted        error

	closing   bool // Close has begun; admit no more records
	closeOnce sync.Once
	closeErr  error

	released  chan struct{} // wakes Append waiters when budget frees
	uploadSem chan struct{} // bounds concurrent segment uploads within a flush
	flushNow  chan struct{}
	stop      chan struct{}
	stopped   chan struct{}
}

// NewProducer constructs a producer and claims the log by bumping the manifest
// epoch. A successful return means this node owns the log at its claimed epoch.
func NewProducer(ctx context.Context, os objectstore.ObjectStore, cfg ProducerConfig) (*Producer, error) {
	cfg.withDefaults()
	runID := ulid.Make().String()
	log := cfg.Logger.With("wal_manifest", cfg.ManifestPath, "wal_run_id", runID)
	p := &Producer{
		store:     NewStore(os, cfg.ManifestPath),
		os:        os,
		cfg:       cfg,
		now:       time.Now,
		log:       log,
		metrics:   newProducerMetrics(log, cfg.Meter),
		runID:     runID,
		released:  make(chan struct{}, 1),
		uploadSem: make(chan struct{}, cfg.UploadConcurrency),
		flushNow:  make(chan struct{}, 1),
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	if err := p.claim(ctx); err != nil {
		return nil, err
	}
	//nolint:gosec // run() is a background loop scoped to Close(), not to NewProducer's ctx
	go p.run()
	return p, nil
}

// Epoch returns the epoch this producer claimed.
func (p *Producer) Epoch() uint64 { return p.epoch }

// claim bumps the manifest epoch under CAS, establishing this producer as the
// current primary.
//
// Every failure - lost race, transient error, or an ambiguous PUT whose
// response was lost - retries the same way: reload, bump, re-CAS. Safe without
// a claimant identity because a claim only succeeds on a clean CAS, and the
// retry's higher epoch supersedes any earlier attempt that silently landed.
func (p *Producer) claim(ctx context.Context) error {
	bo := newBackoff(p.cfg.ManifestInitialBackoff)
	var last error
	for attempt := 0; attempt < p.cfg.MaxClaimAttempts; attempt++ {
		m, ver, ok, err := p.store.Load(ctx)
		if err != nil {
			last = fmt.Errorf("wal: claim load: %w", err)
			if serr := bo.sleep(ctx); serr != nil {
				return serr
			}
			continue
		}
		myEpoch := m.Epoch() + 1
		m.SetEpoch(myEpoch)
		var cErr error
		if ok {
			cErr = p.store.Commit(ctx, m, ver)
		} else {
			cErr = p.store.Create(ctx, m)
		}
		if cErr == nil {
			p.epoch = myEpoch
			p.log.Info("claimed epoch", "epoch", myEpoch, "attempt", attempt)
			p.metrics.claims.Add(ctx, 1, metric.WithAttributes(attrOutcomeOK))
			return nil
		}
		last = fmt.Errorf("wal: claim commit: %w", cErr)
		p.metrics.claimRetries.Add(ctx, 1)
		// A lost race is a free immediate re-plan; anything else backs off.
		if !errors.Is(cErr, objectstore.ErrPreconditionFailed) && !errors.Is(cErr, objectstore.ErrAlreadyExists) {
			p.log.Warn("claim commit failed, backing off", "attempt", attempt, "err", cErr)
			if serr := bo.sleep(ctx); serr != nil {
				return serr
			}
		}
	}
	p.log.Error("claim exhausted attempts", "attempts", p.cfg.MaxClaimAttempts, "err", last)
	p.metrics.claims.Add(ctx, 1, metric.WithAttributes(attrOutcomeError))
	return fmt.Errorf("wal: claim exceeded %d attempts: %w", p.cfg.MaxClaimAttempts, last)
}

// Append enqueues a group of framed records with an optional metadata payload.
// It BLOCKS when the in-flight byte (or batch-count) budget is exhausted, until
// a flush frees space or ctx is cancelled - this is the producer's backpressure
// onto the caller. The returned Durability resolves when the group is
// committed. Records are not copied; do not mutate them until it resolves.
func (p *Producer) Append(ctx context.Context, records [][]byte, meta []byte) (*Durability, error) {
	if len(records) == 0 {
		return nil, ErrNoRecords
	}
	w := newDurability(len(records))
	b := 0
	for _, r := range records {
		b += len(r)
	}
	for {
		p.mu.Lock()
		if p.halted != nil {
			err := p.halted
			p.mu.Unlock()
			return nil, err
		}
		// Set before the run loop stops, so no record is admitted after the
		// final drain and left with a Durability nothing resolves.
		if p.closing {
			p.mu.Unlock()
			return nil, ErrHalted
		}
		// Admit if nothing is in flight (so an oversized lone Append can never
		// deadlock), else require both budgets to have room.
		fits := p.inFlightCount == 0 ||
			(p.inFlightBytes+b <= p.cfg.MaxInFlightBytes && p.inFlightCount+1 <= p.cfg.MaxInFlightBatches)
		if fits {
			p.pending = append(p.pending, pendingItem{records: records, meta: meta, bytes: b, w: w})
			p.pendByte += b
			p.inFlightBytes += b
			p.inFlightCount++
			overSize := p.cfg.FlushBytes > 0 && p.pendByte >= p.cfg.FlushBytes
			room := p.hasRoomLocked()
			p.mu.Unlock()
			if overSize {
				p.signal(p.flushNow)
			}
			if room {
				p.signal(p.released) // baton: wake the next waiter if budget remains
			}
			p.metrics.appendRecords.Add(ctx, int64(len(records)))
			p.metrics.appendBytes.Add(ctx, int64(b))
			return w, nil
		}
		p.mu.Unlock()
		select {
		case <-p.released:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.stopped:
			return nil, ErrHalted
		}
	}
}

func (p *Producer) hasRoomLocked() bool {
	return p.inFlightCount == 0 ||
		(p.inFlightBytes < p.cfg.MaxInFlightBytes && p.inFlightCount < p.cfg.MaxInFlightBatches)
}

func (p *Producer) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Close stops the flush loop, drains buffered records, and halts the producer
// so later Appends fail fast. Idempotent; later calls return the first result.
//
// closing is set before the run loop stops so every admitted record is in
// p.pending when the final drain runs.
func (p *Producer) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()

		close(p.stop)
		<-p.stopped
		p.closeErr = p.flush(ctx)
		p.halt(ErrHalted)
	})
	return p.closeErr
}

func (p *Producer) run() {
	defer close(p.stopped)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			_ = p.flush(context.Background())
		case <-p.flushNow:
			_ = p.flush(context.Background())
		}
	}
}

// flush drains buffered records, rotates them into size-capped segments,
// uploads each, and CAS-appends their entries - coalescing up to
// ManifestAppendBatchSize entries per commit. In-flight budget for an Append is
// released only once its segment is resolved (committed or failed).
func (p *Producer) flush(ctx context.Context) error {
	p.mu.Lock()
	if p.halted != nil {
		batch := p.takeLocked()
		p.mu.Unlock()
		p.failItems(batch, p.halted)
		return p.halted
	}
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := p.takeLocked()
	p.mu.Unlock()

	plans := p.planSegments(batch)

	// Reserve a contiguous ordinal range and name the segments.
	p.mu.Lock()
	base := p.ordinal
	p.ordinal += uint64(len(plans))
	p.mu.Unlock()
	for i := range plans {
		plans[i].location = fmt.Sprintf("%s/%s/%016x", p.cfg.SegmentPrefix, p.runID, base+uint64(i))
	}

	// Upload segments concurrently, bounded by UploadConcurrency. Each goroutine
	// writes only its own result slot; the WaitGroup provides the happens-before
	// for reading them, so no lock is needed on results.
	results := make([]error, len(plans))
	var wg sync.WaitGroup
	for i := range plans {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.uploadSem <- struct{}{}
			defer func() { <-p.uploadSem }()
			seg, err := encodeSegment(plans[i].records, p.cfg.Compression, p.cfg.LegacySegmentFormat)
			if err != nil {
				results[i] = err
				return
			}
			loc := plans[i].location
			results[i] = retryWithBackoff(ctx, p.cfg.UploadMaxAttempts, p.cfg.UploadInitialBackoff, func() error {
				return p.os.Put(ctx, loc, seg)
			})
		}(i)
	}
	wg.Wait()

	// To preserve total order we commit only the contiguous successful PREFIX
	// and refuse to commit at or after the first failed upload: committing a
	// later segment ahead of a failed earlier one would reorder the log. A disk
	// WAL likewise stops the log on a write error rather than skipping ahead.
	firstFail := len(plans)
	for i, e := range results {
		if e != nil {
			firstFail = i
			break
		}
	}

	committed, cerr := p.commitInOrder(ctx, plans[:firstFail])
	if cerr != nil {
		// Manifest unwritable (fenced or transient exhausted) is fatal: halt and
		// fail everything not committed (rest of the prefix and the suffix).
		p.halt(cerr)
		for _, pl := range plans[committed:] {
			p.failItems(pl.items, cerr)
		}
		return cerr
	}

	if firstFail < len(plans) {
		// A permanent upload failure halts the producer: the log cannot advance
		// past a gap without reordering. Callers resubmit from the failure point
		// (typically after failover to a fresh producer at a new epoch).
		uerr := fmt.Errorf("wal: segment upload failed at ordinal %d after %d attempts, halting to preserve log order: %w",
			base+uint64(firstFail), p.cfg.UploadMaxAttempts, results[firstFail])
		p.metrics.uploadFailures.Add(ctx, 1)
		p.halt(uerr)
		for _, pl := range plans[firstFail:] {
			p.failItems(pl.items, uerr)
		}
		return uerr
	}
	p.log.Debug("flush committed", "segments", len(plans), "records", len(batch))
	return nil
}

// commitInOrder commits plans in strict ordinal order, coalescing up to
// ManifestAppendBatchSize entries per CAS (0 = all in one). It returns the
// count successfully committed and the error that stopped it (nil if all did).
// Because commits run only here, from a single flush at a time, the manifest's
// linear history always matches ordinal (and therefore Append) order.
func (p *Producer) commitInOrder(ctx context.Context, plans []segPlan) (int, error) {
	mabs := p.cfg.ManifestAppendBatchSize
	i := 0
	for i < len(plans) {
		end := len(plans)
		if mabs > 0 && i+mabs < end {
			end = i + mabs
		}
		group := plans[i:end]
		seqs, err := p.commitEntries(ctx, group)
		if err != nil {
			return i, err
		}
		for gi, pl := range group {
			p.resolvePlan(pl, seqs[gi])
			p.metrics.batchRecords.Record(ctx, int64(len(pl.records)))
			p.metrics.batchBytes.Record(ctx, int64(pl.bytes))
		}
		i = end
	}
	return len(plans), nil
}

// planSegments packs Append groups into segments of at most SegmentMaxBytes,
// keeping each group whole. A group larger than the cap gets its own segment.
func (p *Producer) planSegments(batch []pendingItem) []segPlan {
	nowMs := p.now().UnixMilli()
	cap := p.cfg.SegmentMaxBytes
	var plans []segPlan
	var cur segPlan
	var curIdx uint32
	seal := func() {
		if len(cur.items) > 0 {
			plans = append(plans, cur)
			cur = segPlan{}
			curIdx = 0
		}
	}
	for _, it := range batch {
		if cap > 0 && len(cur.items) > 0 && cur.bytes+it.bytes > cap {
			seal()
		}
		cur.metas = append(cur.metas, RecordMeta{StartIndex: curIdx, IngestionTimeMs: nowMs, Payload: it.meta})
		cur.records = append(cur.records, it.records...)
		cur.items = append(cur.items, it)
		cur.bytes += it.bytes
		curIdx += uint32(len(it.records))
	}
	seal()
	return plans
}

// commitEntries CAS-appends every plan's entry in a single manifest commit,
// returning the assigned sequences aligned to plans. Retry/precondition/fencing
// semantics match the single-entry path.
func (p *Producer) commitEntries(ctx context.Context, plans []segPlan) (seqs []uint64, err error) {
	start := p.now()
	defer func() {
		p.metrics.commitDuration.Record(ctx, p.now().Sub(start).Seconds())
		outcome := attrOutcomeOK
		if err != nil {
			outcome = attrOutcomeError
		}
		p.metrics.commits.Add(ctx, 1, metric.WithAttributes(outcome))
	}()
	bo := newBackoff(p.cfg.ManifestInitialBackoff)
	cbo := newBackoff(p.cfg.ManifestInitialBackoff)
	transient := 0
	conflicts := 0
	// Set once an attempt fails in a way that does not prove the PUT did not
	// land (timeout, reset, 5xx). After that, every retry must check first.
	unknown := false
	for {
		m, ver, _, err := p.store.Load(ctx)
		if err != nil {
			transient++
			p.metrics.commitRetries.Add(ctx, 1, metric.WithAttributes(attrReasonLoad))
			if transient >= p.cfg.ManifestMaxAttempts {
				p.log.Warn("commit exhausted attempts loading the manifest", "attempts", transient, "err", err)
				return nil, fmt.Errorf("wal: commit load after %d attempts: %w", transient, err)
			}
			if serr := bo.sleep(ctx); serr != nil {
				return nil, serr
			}
			continue
		}
		if m.Epoch() != p.epoch {
			p.log.Warn("producer fenced", "manifest_epoch", m.Epoch(), "producer_epoch", p.epoch)
			p.metrics.fenced.Add(ctx, 1)
			return nil, ErrFenced
		}
		// Segment locations are unique per (runID, ordinal), so a lost
		// response resolves by lookup rather than re-appending.
		if unknown {
			seqs, ok, herr := harvestCommitted(m, plans)
			if herr != nil {
				p.log.Error("commit group partially present in manifest", "err", herr)
				return nil, herr
			}
			if ok {
				p.log.Info("ambiguous commit resolved: entries already landed", "sequences", seqs)
				return seqs, nil
			}
		}
		seqs := make([]uint64, len(plans))
		for i, pl := range plans {
			s, e := m.Append(pl.location, pl.metas, len(pl.records))
			if e != nil {
				return nil, e // encoding error: not retryable
			}
			seqs[i] = s
		}
		// Fencing is enforced by the CAS ETag; this keeps the epoch field in
		// agreement so a broken ETag path cannot silently disable it.
		if m.Epoch() != p.epoch {
			p.log.Error("epoch invariant violated", "manifest_epoch", m.Epoch(), "producer_epoch", p.epoch)
			return nil, fmt.Errorf("wal: epoch invariant violated: manifest %d, producer %d", m.Epoch(), p.epoch)
		}
		err = p.store.Commit(ctx, m, ver)
		if err == nil {
			return seqs, nil
		}
		// 412: definitively did not land. Re-plan, but bounded and backed off.
		if errors.Is(err, objectstore.ErrPreconditionFailed) {
			conflicts++
			p.metrics.commitRetries.Add(ctx, 1, metric.WithAttributes(attrReasonCASConflict))
			if conflicts >= p.cfg.MaxCommitConflicts {
				p.log.Warn("commit exhausted CAS conflict budget", "conflicts", conflicts)
				return nil, fmt.Errorf("wal: commit lost %d CAS races (contended)", conflicts)
			}
			if serr := cbo.sleep(ctx); serr != nil {
				return nil, serr
			}
			continue
		}
		// 409: concurrent conditional write in flight. Did not land; retry
		// with backoff rather than re-planning.
		if errors.Is(err, objectstore.ErrConflict) {
			transient++
			p.metrics.commitRetries.Add(ctx, 1, metric.WithAttributes(attrReasonWriteConflict))
			if transient >= p.cfg.ManifestMaxAttempts {
				p.log.Warn("commit exhausted attempts on repeated conflict", "attempts", transient, "err", err)
				return nil, fmt.Errorf("wal: commit conflict after %d attempts: %w", transient, err)
			}
			if serr := bo.sleep(ctx); serr != nil {
				return nil, serr
			}
			continue
		}
		// Anything else: outcome unknown. The PUT may have landed.
		unknown = true
		transient++
		p.metrics.commitRetries.Add(ctx, 1, metric.WithAttributes(attrReasonUnknown))
		if transient >= p.cfg.ManifestMaxAttempts {
			p.log.Warn("commit exhausted attempts, outcome unknown", "attempts", transient, "err", err)
			return nil, fmt.Errorf("wal: commit after %d attempts: %w", transient, err)
		}
		if serr := bo.sleep(ctx); serr != nil {
			return nil, serr
		}
	}
}

// harvestCommitted reports whether plans' entries are already in m from an
// attempt whose response was lost, returning their assigned sequences. One CAS
// carries the whole group, so partial presence is an error, not a re-commit.
func harvestCommitted(m *Manifest, plans []segPlan) ([]uint64, bool, error) {
	tail, err := m.TailLocations(len(plans) * 4)
	if err != nil {
		return nil, false, err
	}
	seqs := make([]uint64, len(plans))
	found := 0
	for i, pl := range plans {
		if e, ok := tail[pl.location]; ok {
			seqs[i] = e.Sequence
			found++
		}
	}
	switch found {
	case 0:
		return nil, false, nil
	case len(plans):
		return seqs, true, nil
	default:
		return nil, false, fmt.Errorf("wal: manifest holds %d/%d entries of an atomic commit group", found, len(plans))
	}
}

func (p *Producer) takeLocked() []pendingItem {
	batch := p.pending
	p.pending = nil
	p.pendByte = 0
	return batch
}

// release returns in-flight budget and wakes a blocked Append.
func (p *Producer) release(bytes, count int) {
	p.mu.Lock()
	p.inFlightBytes -= bytes
	p.inFlightCount -= count
	room := p.hasRoomLocked()
	p.mu.Unlock()
	if room {
		p.signal(p.released)
	}
}

// resolvePlan resolves each Append in a committed segment to the sequence of
// its own first record (baseSeq + the group's StartIndex within the segment),
// not the segment base - so a caller that coalesced behind others still learns
// where its records actually landed in the sequence space.
func (p *Producer) resolvePlan(pl segPlan, baseSeq uint64) {
	var b, c int
	for k, it := range pl.items {
		recSeq := baseSeq
		if k < len(pl.metas) {
			recSeq = baseSeq + uint64(pl.metas[k].StartIndex)
		}
		it.w.resolve(recSeq, nil)
		b += it.bytes
		c++
	}
	p.release(b, c)
}

func (p *Producer) failItems(items []pendingItem, err error) {
	var b, c int
	for _, it := range items {
		it.w.resolve(0, err)
		b += it.bytes
		c++
	}
	p.release(b, c)
}

func (p *Producer) halt(err error) {
	p.mu.Lock()
	first := p.halted == nil
	if first {
		p.halted = err
	}
	p.mu.Unlock()
	if !first {
		return
	}
	if errors.Is(err, ErrHalted) {
		p.log.Info("producer closed")
		p.metrics.halted.Add(context.Background(), 1, metric.WithAttributes(attrOutcomeOK))
		return
	}
	p.log.Error("producer halted", "err", err)
	p.metrics.halted.Add(context.Background(), 1, metric.WithAttributes(attrOutcomeError))
}

const maxBackoff = 5 * time.Second

// backoff produces exponentially increasing sleep durations with full jitter,
// capped at maxBackoff. Shared by the upload and manifest-commit retry paths.
type backoff struct {
	next time.Duration
}

func newBackoff(base time.Duration) backoff {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	return backoff{next: base}
}

func (b *backoff) sleep(ctx context.Context) error {
	d := time.Duration(rand.Int63n(int64(b.next) + 1))
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}
	if b.next < maxBackoff {
		b.next *= 2
		if b.next > maxBackoff {
			b.next = maxBackoff
		}
	}
	return nil
}

func retryWithBackoff(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	bo := newBackoff(base)
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		if serr := bo.sleep(ctx); serr != nil {
			return serr
		}
	}
	return err
}
