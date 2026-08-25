package wal_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// prefixOracle tails the log with a real wal.Replica and asserts its applied
// state is always an exact prefix of the committed log. The porcupine harness
// reads the manifest directly and never exercises the replica path.
type prefixOracle struct {
	mu      sync.Mutex
	applied []string
	seqs    []uint64
	err     error
}

func (o *prefixOracle) Apply(_ context.Context, rec wal.Record) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Sequences must be non-decreasing across records and strictly increasing
	// across segments; a segment's records share its sequence.
	if n := len(o.seqs); n > 0 && rec.Sequence < o.seqs[n-1] {
		o.err = errAt("replica applied seq %d after %d (went backwards)", rec.Sequence, o.seqs[n-1])
	}
	o.applied = append(o.applied, string(rec.GroupMeta))
	o.seqs = append(o.seqs, rec.Sequence)
	return nil
}

func (o *prefixOracle) snapshot() (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := ""
	for _, id := range o.applied {
		s = push(s, id)
	}
	return s, o.err
}

func errAt(f string, a ...interface{}) error { return fmt.Errorf(f, a...) }

// TestFailover_ZombiePrimary tests fencing. Appenders write continuously while
// a chaos goroutine constructs a new producer - bumping the epoch and fencing
// the incumbent - then abandons the old one without Close(), leaving its flush
// loop running: a zombie primary that has not noticed it lost the log.
//
// If fencing leaks, a zombie commit lands out of order or duplicates an id and
// no linearization exists. A real replica tails concurrently.
func TestFailover_ZombiePrimary(t *testing.T) {
	for _, seed := range seeds(t) {
		runFailover(t, seed)
	}
}

func runFailover(t *testing.T, seed uint64) {
	t.Helper()
	const (
		clients   = 3
		perClient = 30
		failovers = 5
	)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	inner, keyPrefix := backingStore(t)
	manifest := keyPrefix + "wal/manifest"
	segPrefix := keyPrefix + "wal/seg"

	sim := objectstore.NewSimStore(inner, seed, objectstore.Faults{
		FailAmbiguous: 0.10,
		FailClean:     0.05,
		KeySubstring:  "wal/",
	})

	newProducer := func() (*wal.Producer, error) {
		return wal.NewProducer(ctx, sim, wal.ProducerConfig{
			ManifestPath:           manifest,
			SegmentPrefix:          segPrefix,
			FlushInterval:          2 * time.Millisecond,
			ManifestInitialBackoff: 200 * time.Microsecond,
			UploadInitialBackoff:   200 * time.Microsecond,
			ManifestMaxAttempts:    8,
		})
	}

	p, err := newProducer()
	if err != nil {
		t.Fatalf("seed %d: initial producer: %v", seed, err)
	}
	var pmu sync.RWMutex
	cur := p
	var zombies []*wal.Producer
	epochs := []uint64{p.Epoch()}

	rec := &recorder{}
	stop := make(chan struct{})

	// Replica: real tail loop, real cursor, real applier.
	oracle := &prefixOracle{}
	replica := wal.NewReplica(inner, oracle, wal.ReplicaConfig{
		ManifestPath: manifest,
		PollInterval: 2 * time.Millisecond,
		Cursor:       &wal.MemCursorStore{},
	})
	var repWG sync.WaitGroup
	repWG.Add(1)
	go func() {
		defer repWG.Done()
		for {
			select {
			case <-stop:
				_, _ = replica.Poll(ctx) // final drain
				return
			default:
			}
			_, _ = replica.Poll(ctx)
			time.Sleep(time.Millisecond)
		}
	}()

	// Chaos: fence the incumbent, abandon it running.
	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		for i := 0; i < failovers; i++ {
			select {
			case <-stop:
				return
			case <-time.After(35 * time.Millisecond):
			}
			np, err := newProducer()
			if err != nil {
				continue
			}
			pmu.Lock()
			zombies = append(zombies, cur) // deliberately NOT Closed
			cur = np
			epochs = append(epochs, np.Epoch())
			pmu.Unlock()
		}
	}()

	// Appenders. On any error the id is recorded as UNKNOWN and we move to a
	// fresh id - retrying the same id would be a legitimate at-least-once
	// duplicate and is not what this test is probing.
	var appWG sync.WaitGroup
	for c := 0; c < clients; c++ {
		appWG.Add(1)
		go func(c int) {
			defer appWG.Done()
			for i := 0; i < perClient; i++ {
				id := "f" + strconv.Itoa(c) + "_" + strconv.Itoa(i)
				in := logInput{op: opAppend, id: id}
				call := now()
				pmu.RLock()
				prod := cur
				pmu.RUnlock()
				d, err := prod.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
				if err != nil {
					rec.add(c, in, call, logOutput{seq: seqUnknown}, now())
					time.Sleep(3 * time.Millisecond)
					continue
				}
				seq, werr := d.Wait(ctx)
				if werr != nil {
					rec.add(c, in, call, logOutput{seq: seqUnknown}, now())
					time.Sleep(3 * time.Millisecond)
					continue
				}
				rec.add(c, in, call, logOutput{seq: seq}, now())

				// Interleave a replica-prefix check.
				if i%7 == 0 {
					committed, rerr := readLog(ctx, inner, manifest)
					if rerr == nil {
						got, oerr := oracle.snapshot()
						if oerr != nil {
							t.Errorf("seed %d: %v", seed, oerr)
						}
						if !strings.HasPrefix(committed, got) {
							t.Errorf("seed %d: replica state is NOT a prefix of the committed log\n  replica:   %s\n  committed: %s", seed, got, committed)
						}
					}
				}
			}
		}(c)
	}

	appWG.Wait()
	chaosWG.Wait()
	close(stop)
	repWG.Wait()

	pmu.RLock()
	final := cur
	zs := append([]*wal.Producer(nil), zombies...)
	allEpochs := append([]uint64(nil), epochs...)
	pmu.RUnlock()
	_ = final.Close(ctx)
	for _, z := range zs {
		_ = z.Close(ctx)
	}

	// Epochs must be strictly increasing: no two producers ever claimed the same one.
	for i := 1; i < len(allEpochs); i++ {
		if allEpochs[i] <= allEpochs[i-1] {
			t.Fatalf("seed %d: epoch did not advance: %v", seed, allEpochs)
		}
	}

	call := now()
	committed, err := readLog(ctx, inner, manifest)
	if err != nil {
		t.Fatalf("seed %d: final read: %v", seed, err)
	}
	rec.add(0, logInput{op: opRead}, call, logOutput{log: committed}, now())

	// Final replica drain, then the prefix property must hold exactly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = replica.Poll(ctx)
		got, _ := oracle.snapshot()
		if got == committed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, oerr := oracle.snapshot()
	if oerr != nil {
		t.Fatalf("seed %d: replica ordering: %v", seed, oerr)
	}
	if !strings.HasPrefix(committed, got) {
		t.Fatalf("seed %d: final replica state is not a prefix of the log\n  replica:   %s\n  committed: %s", seed, got, committed)
	}

	res := porcupine.CheckOperations(logModel.ToModel(), rec.ops)
	st := sim.Stats()
	if !res {
		t.Fatalf("seed %d: NOT LINEARIZABLE across %d failovers (epochs %v, %d ambiguous faults)\n  log: %s",
			seed, failovers, allEpochs, st.AmbiguousFaults, committed)
	}
	t.Logf("seed %d OK: %d ops, epochs %v, %d ambiguous, %d clean, log=%d, replica applied=%d",
		seed, len(rec.ops), allEpochs, st.AmbiguousFaults, st.CleanFaults, size(committed), size(got))
}
