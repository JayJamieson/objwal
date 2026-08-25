package wal_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// SUSPECT 6: in-flight budget accounting.
//
// Append reserves inFlightBytes/inFlightCount and blocks on p.released when
// both budgets are full. Every take path must eventually release exactly what
// it reserved, through resolvePlan or failItems. If any path leaks a
// reservation, the budget never recovers and Append blocks FOREVER - the
// admission rule only bypasses the budget when inFlightCount == 0.
//
// Driven with a deliberately tiny budget so almost every Append must wait, and
// with faults forcing halts, upload failures and failovers so the error paths
// carry reservations too.
func TestBudgetAccounting_NeverWedges(t *testing.T) {
	for _, seed := range seeds(t) {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			inner, keyPrefix := backingStore(t)
			manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
			sim := objectstore.NewSimStore(inner, seed, objectstore.Faults{
				FailClean:     0.15,
				FailAmbiguous: 0.15,
				KeySubstring:  keyPrefix,
			})

			mk := func() (*wal.Producer, error) {
				return wal.NewProducer(ctx, sim, wal.ProducerConfig{
					ManifestPath:  manifest,
					SegmentPrefix: segPrefix,
					FlushInterval: 3 * time.Millisecond,
					// Tiny budget: at most 2 Appends and ~64 bytes in flight.
					MaxInFlightBytes:       64,
					MaxInFlightBatches:     2,
					UploadMaxAttempts:      2,
					UploadInitialBackoff:   200 * time.Microsecond,
					ManifestInitialBackoff: 200 * time.Microsecond,
					ManifestMaxAttempts:    6,
				})
			}
			p, err := mk()
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			var pmu sync.Mutex
			all := []*wal.Producer{p}
			defer func() {
				for _, q := range all {
					_ = q.Close(context.Background())
				}
			}()

			var wg sync.WaitGroup
			wedged := make(chan string, 128)
			for c := 0; c < 4; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					for i := 0; i < 25; i++ {
						id := "b" + strconv.Itoa(c) + "_" + strconv.Itoa(i)
						pmu.Lock()
						cur := all[len(all)-1]
						pmu.Unlock()

						// Every Append must either be admitted or rejected
						// within a generous bound. Blocking past it means the
						// budget never came back.
						actx, acancel := context.WithTimeout(ctx, 10*time.Second)
						d, aerr := cur.Append(actx, [][]byte{[]byte(id + "-payload")}, []byte(id))
						acancel()
						if errors.Is(aerr, context.DeadlineExceeded) {
							wedged <- id
							return
						}
						if aerr != nil {
							// halted: fail over
							pmu.Lock()
							if all[len(all)-1] == cur {
								if np, nerr := mk(); nerr == nil {
									all = append(all, np)
								}
							}
							pmu.Unlock()
							continue
						}
						wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
						_, werr := d.Wait(wctx)
						wcancel()
						if errors.Is(werr, context.DeadlineExceeded) {
							wedged <- id + "(wait)"
							return
						}
					}
				}(c)
			}
			wg.Wait()
			close(wedged)
			var stuck []string
			for id := range wedged {
				stuck = append(stuck, id)
			}
			if len(stuck) > 0 {
				t.Fatalf("seed %d: BUDGET LEAK - %d operations blocked indefinitely (%v). "+
					"A take path reserved in-flight budget without releasing it.", seed, len(stuck), stuck)
			}
		}()
	}
}

// SUSPECT 7: an Append larger than SegmentMaxBytes, and an Append larger than
// the entire in-flight budget.
//
// The admission rule bypasses the budget only when inFlightCount == 0, so an
// oversized lone Append is meant to always get through. Under contention it
// must still not starve forever. Separately, a group must never be SPLIT
// across segments: resolvePlan hands the whole group one base sequence, so a
// split would silently misreport where a group's records landed.
func TestOversizedAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, keyPrefix := backingStore(t)
	manifest, segPrefix := keyPrefix+"m", keyPrefix+"seg"
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:       manifest,
		SegmentPrefix:      segPrefix,
		FlushInterval:      5 * time.Millisecond,
		SegmentMaxBytes:    32,  // far smaller than the big group
		MaxInFlightBytes:   128, // smaller than the big group
		MaxInFlightBatches: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	big := make([][]byte, 6)
	for i := range big {
		buf := make([]byte, 200)
		for j := range buf {
			buf[j] = byte('A' + i)
		}
		big[i] = buf
	}

	// Small appends racing alongside, so the oversized one is not alone.
	var wg sync.WaitGroup
	for c := 0; c < 3; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				id := "s" + strconv.Itoa(c) + "_" + strconv.Itoa(i)
				d, err := p.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
				if err != nil {
					t.Errorf("small append: %v", err)
					return
				}
				if _, err := d.Wait(ctx); err != nil {
					t.Errorf("small wait: %v", err)
					return
				}
			}
		}(c)
	}

	d, err := p.Append(ctx, big, []byte("BIG"))
	if err != nil {
		t.Fatalf("OVERSIZED APPEND REJECTED: %v", err)
	}
	baseSeq, err := d.Wait(ctx)
	if err != nil {
		t.Fatalf("OVERSIZED APPEND NOT DURABLE: %v", err)
	}
	wg.Wait()
	_ = p.Close(ctx)

	// The group's 6 records must occupy [baseSeq, baseSeq+6) contiguously and
	// in order - i.e. the group was not split across segments.
	seen := map[uint64]string{}
	r := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
		seen[rec.Sequence] = string(rec.Data[:1])
		return nil
	}), wal.ReplicaConfig{ManifestPath: manifest, PollInterval: time.Millisecond})
	for i := 0; i < 8; i++ {
		if _, err := r.Poll(ctx); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	for k := uint64(0); k < 6; k++ {
		want := string(rune('A' + k))
		got, ok := seen[baseSeq+k]
		if !ok {
			t.Fatalf("GROUP SPLIT OR MISSEQUENCED: no record at seq %d (group base %d)", baseSeq+k, baseSeq)
		}
		if got != want {
			t.Fatalf("GROUP SPLIT OR MISSEQUENCED: seq %d holds %q, want %q (group base %d)",
				baseSeq+k, got, want, baseSeq)
		}
	}
}

// SUSPECT 8: Durability resolution under concurrent and repeated Wait.
func TestDurability_ConcurrentAndRepeatedWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, keyPrefix := backingStore(t)
	p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
		ManifestPath:  keyPrefix + "m",
		SegmentPrefix: keyPrefix + "seg",
		FlushInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close(ctx) }()

	for i := 0; i < 20; i++ {
		d, err := p.Append(ctx, [][]byte{[]byte("w" + strconv.Itoa(i))}, []byte("w"))
		if err != nil {
			t.Fatal(err)
		}
		// Many concurrent waiters on one handle, all must agree.
		var wg sync.WaitGroup
		results := make([]uint64, 8)
		errs := make([]error, 8)
		for k := 0; k < 8; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				results[k], errs[k] = d.Wait(ctx)
			}(k)
		}
		wg.Wait()
		for k := 1; k < 8; k++ {
			if results[k] != results[0] || (errs[k] == nil) != (errs[0] == nil) {
				t.Fatalf("concurrent Wait disagreed: %d/%v vs %d/%v", results[k], errs[k], results[0], errs[0])
			}
		}
		// Repeated Wait after resolution must be stable, not block or panic.
		for k := 0; k < 3; k++ {
			s2, e2 := d.Wait(ctx)
			if s2 != results[0] || (e2 == nil) != (errs[0] == nil) {
				t.Fatalf("repeated Wait changed answer: %d/%v then %d/%v", results[0], errs[0], s2, e2)
			}
		}
	}
}
