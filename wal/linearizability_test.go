package wal_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// Model: an exactly-once, totally ordered log of record ids. State is the
// committed log, encoded as "|a|b|c|" so it is == comparable.
//
//	append(id) -> seq   legal only if id is uncommitted and seq is its position
//	append(id) -> ???   outcome unknown; branches into landed and not-landed,
//	                    never into landed-twice
//	read()     -> log   atomic manifest read; must equal state exactly, which
//	                    prunes the branching
const (
	opAppend = "append"
	opRead   = "read"

	seqUnknown = ^uint64(0) // append returned an error: outcome unknown
)

type logInput struct {
	op string
	id string
}

type logOutput struct {
	seq uint64 // for append
	log string // for read
}

func has(state, id string) bool { return strings.Contains(state, "|"+id+"|") }

func size(state string) int {
	if state == "" {
		return 0
	}
	return strings.Count(state, "|") - 1
}

func push(state, id string) string {
	if state == "" {
		return "|" + id + "|"
	}
	return state + id + "|"
}

var logModel = porcupine.NondeterministicModel{
	Init: func() []any { return []any{""} },
	Step: func(st, in, out any) []any {
		state := st.(string)
		i := in.(logInput)
		o := out.(logOutput)
		switch i.op {
		case opAppend:
			if o.seq == seqUnknown {
				// Unknown outcome: it may have landed at the end, or not at
				// all. It may never have landed twice.
				if has(state, i.id) {
					return []any{state}
				}
				return []any{state, push(state, i.id)}
			}
			// A definite success. Illegal if the id is already committed
			// (that is the duplicate bug) or if the returned sequence is not
			// its true position in the total order.
			if has(state, i.id) || o.seq != uint64(size(state)) {
				return nil
			}
			return []any{push(state, i.id)}
		case opRead:
			if state != o.log {
				return nil
			}
			return []any{state}
		}
		return nil
	},
	DescribeOperation: func(in, out any) string {
		i := in.(logInput)
		o := out.(logOutput)
		if i.op == opRead {
			return fmt.Sprintf("read() -> %s", o.log)
		}
		if o.seq == seqUnknown {
			return fmt.Sprintf("append(%s) -> ???", i.id)
		}
		return fmt.Sprintf("append(%s) -> %d", i.id, o.seq)
	},
	DescribeState: func(st any) string { return st.(string) },
}

// readLog reads the committed log atomically: one manifest GET, then the
// ordered per-group metadata payloads carrying the record ids.
func readLog(ctx context.Context, os objectstore.ObjectStore, path string) (string, error) {
	m, _, ok, err := wal.NewStore(os, path).Load(ctx)
	if err != nil || !ok {
		return "", err
	}
	entries, err := m.Entries()
	if err != nil {
		return "", err
	}
	state := ""
	for _, e := range entries {
		for _, md := range e.Metadata {
			state = push(state, string(md.Payload))
		}
	}
	return state, nil
}

type recorder struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (r *recorder) add(client int, in logInput, call int64, out logOutput, ret int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{
		ClientId: client, Input: in, Call: call, Output: out, Return: ret,
	})
	r.mu.Unlock()
}

func now() int64 { return time.Now().UnixNano() }

// runOnce drives concurrent appenders and readers against a producer whose
// store injects faults on the manifest key, then checks the history. faults
// is applied as given, except KeySubstring is always pinned to the manifest
// path - only the CAS path is the subject here, segment PUTs are not.
func runOnce(t *testing.T, seed uint64, faults objectstore.Faults) {
	t.Helper()
	const (
		clients    = 4
		perClient  = 25
		readerPoll = 700 * time.Microsecond
	)
	inner, keyPrefix := backingStore(t)
	manifest := keyPrefix + "wal/manifest"
	segPrefix := keyPrefix + "wal/seg"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mem := inner
	faults.KeySubstring = manifest
	sim := objectstore.NewSimStore(mem, seed, faults)

	p, err := wal.NewProducer(ctx, sim, wal.ProducerConfig{
		ManifestPath:           manifest,
		SegmentPrefix:          segPrefix,
		FlushInterval:          2 * time.Millisecond,
		ManifestInitialBackoff: 200 * time.Microsecond,
		UploadInitialBackoff:   200 * time.Microsecond,
		ManifestMaxAttempts:    8,
	})
	if err != nil {
		t.Fatalf("seed %d: NewProducer: %v", seed, err)
	}

	rec := &recorder{}
	var appenders, readers sync.WaitGroup

	// Readers: atomic reads of the committed log, straight off the underlying
	// store (the reader is not the system under test, so it sees no faults).
	stopReaders := make(chan struct{})
	for c := 0; c < 2; c++ {
		readers.Add(1)
		go func(c int) {
			defer readers.Done()
			for {
				select {
				case <-stopReaders:
					return
				case <-time.After(readerPoll):
				}
				call := now()
				got, err := readLog(ctx, mem, manifest)
				if err != nil {
					continue
				}
				rec.add(clients+c, logInput{op: opRead}, call, logOutput{log: got}, now())
			}
		}(c)
	}

	// Appenders.
	for c := 0; c < clients; c++ {
		appenders.Add(1)
		go func(c int) {
			defer appenders.Done()
			for i := 0; i < perClient; i++ {
				id := strconv.Itoa(c) + "_" + strconv.Itoa(i)
				in := logInput{op: opAppend, id: id}
				call := now()
				// One record per Append, with the id in the group metadata, so
				// a manifest read can reconstruct the log in model terms.
				d, err := p.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
				if err != nil {
					rec.add(c, in, call, logOutput{seq: seqUnknown}, now())
					return
				}
				seq, werr := d.Wait(ctx)
				if werr != nil {
					// The contract: an error means UNKNOWN, never "not durable".
					rec.add(c, in, call, logOutput{seq: seqUnknown}, now())
					return
				}
				rec.add(c, in, call, logOutput{seq: seq}, now())
			}
		}(c)
	}

	appenders.Wait()
	close(stopReaders)
	readers.Wait()
	_ = p.Close(ctx)

	// A final settled read, after everything has stopped.
	call := now()
	final, err := readLog(ctx, mem, manifest)
	if err != nil {
		t.Fatalf("seed %d: final read: %v", seed, err)
	}
	rec.add(0, logInput{op: opRead}, call, logOutput{log: final}, now())

	st := sim.Stats()
	res, info := porcupine.CheckOperationsVerbose(logModel.ToModel(), rec.ops, 20*time.Second)
	switch res {
	case porcupine.Ok:
		t.Logf("seed %d OK: %d ops, %d ambiguous, %d slow, %d clean, %d corrupt reads, %d CAS conflicts, log=%d entries",
			seed, len(rec.ops), st.AmbiguousFaults, st.SlowFaults, st.CleanFaults, st.CorruptReads, st.CASFailures, size(final))
	case porcupine.Illegal:
		if path := os.Getenv("PORCUPINE_VIZ"); path != "" {
			f, _ := os.Create(path) //nolint:gosec // local debug output path, set by the developer via env var
			_ = porcupine.Visualize(logModel.ToModel(), info, f)
			_ = f.Close()
		}
		t.Fatalf("seed %d: NOT LINEARIZABLE (%d ambiguous faults). final log: %s",
			seed, st.AmbiguousFaults, final)
	default:
		t.Fatalf("seed %d: checker timed out", seed)
	}
}

func seeds(t *testing.T) []uint64 {
	if s := os.Getenv("DST_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return []uint64{v}
	}
	n := 12
	if s := os.Getenv("DST_SEEDS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i)*0x9E3779B97F4A7C15 + 1
	}
	return out
}

// TestLinearizable_NoFaults is the control: the happy path must linearize.
func TestLinearizable_NoFaults(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{})
	}
}

// TestLinearizable_AmbiguousCommits: the manifest CAS sometimes lands and then
// reports failure. A writer that re-appends rather than checking whether its
// entries landed duplicates records, which the model rejects.
func TestLinearizable_AmbiguousCommits(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{FailAmbiguous: 0.25})
	}
}

// TestLinearizable_Mixed adds clean failures, exercising the re-plan path.
func TestLinearizable_Mixed(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{FailAmbiguous: 0.35, FailClean: 0.25})
	}
}

// TestLinearizable_SlowAmbiguousCommits: the manifest CAS lands, then the
// response is slow enough that real wall-clock time passes before it's
// reported lost - unlike FailAmbiguous, which reports the loss instantly.
// This gives a concurrent retry (or a rival claimant) a genuine window to run
// and observe the manifest before the slow caller's PUT is reported failed,
// which FailAmbiguous's synchronous fault can never produce.
func TestLinearizable_SlowAmbiguousCommits(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{
			SlowAmbiguous: 0.30,
			SlowDelay:     5 * time.Millisecond,
			FailClean:     0.10,
		})
	}
}

// TestLinearizable_CorruptReads: manifest GETs occasionally come back with one
// flipped bit. ParseManifest must reject the digest (ErrCorrupt) rather than
// decode garbage into a phantom entry, and because the fault is rolled fresh
// per call, the bounded Load retries in claim/commitEntries must ride through
// it to a clean read rather than exhausting on transient-looking corruption.
func TestLinearizable_CorruptReads(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{Corrupt: 0.20})
	}
}

// TestLinearizable_Latency: no failures at all, just real per-call latency on
// every manifest op. Pins that jittered delay alone - stacked against
// FlushInterval and the backoff timers - cannot desynchronize the protocol
// into a non-linearizable history.
func TestLinearizable_Latency(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, objectstore.Faults{
			Latency:       1 * time.Millisecond,
			LatencyJitter: 800 * time.Microsecond,
		})
	}
}
