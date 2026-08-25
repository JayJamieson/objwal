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

// The model: an exactly-once, totally ordered log of record ids.
//
// State is the committed log, encoded as "|a|b|c|" so it is == comparable and
// membership is a substring test.
//
// Operations:
//
//	append(id) -> seq   the id is placed at position seq; committing the same
//	                    id twice is NOT a legal step, and that is precisely
//	                    the guarantee an ambiguous manifest CAS threatens.
//	append(id) -> ???   outcome unknown (the response was lost). Legal either
//	                    way: nondeterministic step returns both the state that
//	                    accepted it and the one that did not.
//	read()     -> log   an atomic read of the manifest; must equal the state
//	                    exactly, which is what prunes the nondeterminism.
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
	Init: func() []interface{} { return []interface{}{""} },
	Step: func(st, in, out interface{}) []interface{} {
		state := st.(string)
		i := in.(logInput)
		o := out.(logOutput)
		switch i.op {
		case opAppend:
			if o.seq == seqUnknown {
				// Unknown outcome: it may have landed at the end, or not at
				// all. It may never have landed twice.
				if has(state, i.id) {
					return []interface{}{state}
				}
				return []interface{}{state, push(state, i.id)}
			}
			// A definite success. Illegal if the id is already committed
			// (that is the duplicate bug) or if the returned sequence is not
			// its true position in the total order.
			if has(state, i.id) || o.seq != uint64(size(state)) {
				return nil
			}
			return []interface{}{push(state, i.id)}
		case opRead:
			if state != o.log {
				return nil
			}
			return []interface{}{state}
		}
		return nil
	},
	DescribeOperation: func(in, out interface{}) string {
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
	DescribeState: func(st interface{}) string { return st.(string) },
}

// readLog performs an atomic read of the committed log: one GET of the
// manifest, then the ordered per-Append-group metadata payloads, which carry
// the record ids. Coalescing is transparent here - an entry holds one
// RecordMeta per Append group, in order.
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

// runOnce drives concurrent appenders and readers against one producer whose
// object store injects ambiguous failures on the manifest key, then checks the
// resulting history for linearizability.
func runOnce(t *testing.T, seed uint64, ambiguous, clean float64) {
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
	sim := objectstore.NewSimStore(mem, seed, objectstore.Faults{
		FailAmbiguous: ambiguous,
		FailClean:     clean,
		KeySubstring:  manifest, // only the CAS path; segment PUTs are not the subject
	})

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
		t.Logf("seed %d OK: %d ops, %d ambiguous faults, %d clean faults, %d CAS conflicts, log=%d entries",
			seed, len(rec.ops), st.AmbiguousFaults, st.CleanFaults, st.CASFailures, size(final))
	case porcupine.Illegal:
		if path := os.Getenv("PORCUPINE_VIZ"); path != "" {
			f, _ := os.Create(path)
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
		runOnce(t, s, 0, 0)
	}
}

// TestLinearizable_AmbiguousCommits is the real test. The manifest CAS
// sometimes lands and then reports failure - the S3 "timeout after the write
// applied" case. A writer that re-appends instead of checking whether its own
// entries already landed puts the same record in the log twice, which the
// model rejects.
func TestLinearizable_AmbiguousCommits(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, 0.25, 0)
	}
}

// TestLinearizable_Mixed adds clean failures on top, exercising the retry and
// re-plan paths as well.
func TestLinearizable_Mixed(t *testing.T) {
	for _, s := range seeds(t) {
		runOnce(t, s, 0.35, 0.25)
	}
}
