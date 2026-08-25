package wal_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/JayJamieson/objwal/objectstore"
	"github.com/JayJamieson/objwal/wal"
)

// TestCorruption_IsDetected writes a healthy log, corrupts one byte of a
// committed object, and checks the replica rejects it rather than applying
// something wrong. Before segment footer v2 and manifest footer v4, three of
// these four cases were silent.
func TestCorruption_IsDetected(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, store objectstore.ObjectStore, manifest, seg string)
	}{
		{"segment body bit-flip", func(t *testing.T, store objectstore.ObjectStore, _, seg string) {
			res, err := store.Get(context.Background(), seg)
			if err != nil {
				t.Skipf("no segment: %v", err)
			}
			d := append([]byte(nil), res.Data...)
			// Byte 4 is the first record's first payload byte: past the u32
			// length prefix and far from the 7-byte footer, whose compression
			// and version fields ARE structurally validated.
			d[4] ^= 0xFF
			if err := store.Put(context.Background(), seg, d); err != nil {
				t.Fatal(err)
			}
		}},
		{"record length prefix corrupted", func(t *testing.T, store objectstore.ObjectStore, _, seg string) {
			res, err := store.Get(context.Background(), seg)
			if err != nil {
				t.Skipf("no segment: %v", err)
			}
			d := append([]byte(nil), res.Data...)
			d[0] ^= 0x02 // perturb the first record's u32 length
			if err := store.Put(context.Background(), seg, d); err != nil {
				t.Fatal(err)
			}
		}},
		{"segment truncated", func(t *testing.T, store objectstore.ObjectStore, _, seg string) {
			res, err := store.Get(context.Background(), seg)
			if err != nil {
				t.Skipf("no segment: %v", err)
			}
			d := append([]byte(nil), res.Data...)
			if err := store.Put(context.Background(), seg, d[:len(d)-3]); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest footer next_sequence bumped", func(t *testing.T, store objectstore.ObjectStore, manifest, _ string) {
			res, err := store.Get(context.Background(), manifest)
			if err != nil {
				t.Fatal(err)
			}
			d := append([]byte(nil), res.Data...)
			// footer is 22B: entries_count u32 | next_sequence u64 | epoch u64 | version u16
			d[len(d)-18] ^= 0x01 // perturb next_sequence
			if err := store.Put(context.Background(), manifest, d); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			store, keyPrefix := backingStore(t)
			manifest := keyPrefix + "wal/manifest"
			segPrefix := keyPrefix + "wal/seg"

			p, err := wal.NewProducer(ctx, store, wal.ProducerConfig{
				ManifestPath: manifest, SegmentPrefix: segPrefix,
				FlushInterval: 2 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			for i := 0; i < 12; i++ {
				id := "c" + strconv.Itoa(i)
				d, err := p.Append(ctx, [][]byte{[]byte(id)}, []byte(id))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := d.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				want = append(want, id)
			}
			_ = p.Close(ctx)

			// Find a committed segment.
			m, _, _, err := wal.NewStore(store, manifest).Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := m.Entries()
			if err != nil || len(entries) == 0 {
				t.Fatalf("entries: %v", err)
			}
			seg := entries[len(entries)/2].Location

			c.mutate(t, store, manifest, seg)

			// Replay with a fresh replica.
			var got []string
			var applyErr error
			r := wal.NewReplica(store, wal.ApplyFunc(func(_ context.Context, rec wal.Record) error {
				got = append(got, string(rec.Data))
				return nil
			}), wal.ReplicaConfig{ManifestPath: manifest, PollInterval: time.Millisecond})

			for i := 0; i < 5; i++ {
				if _, err := r.Poll(ctx); err != nil {
					applyErr = err
					break
				}
			}

			switch {
			case applyErr != nil:
				t.Logf("DETECTED: replica refused to proceed: %v", applyErr)
			case len(got) != len(want):
				t.Errorf("SILENT CORRUPTION: replica applied %d records, log holds %d — no error raised", len(got), len(want))
			default:
				bad := 0
				for i := range got {
					if got[i] != want[i] {
						bad++
					}
				}
				if bad > 0 {
					t.Errorf("SILENT CORRUPTION: %d/%d records applied with WRONG CONTENT and no error raised", bad, len(want))
				} else {
					t.Logf("survived: corruption did not change observable output")
				}
			}
		})
	}
}
