package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

const seqCnt = 1000

// seqDelete is one way of removing a key from a Map.
type seqDelete struct {
	name string
	fn   func(m *skiplistmap.Map, key string) bool
}

func seqDeletes() []seqDelete {
	return []seqDelete{
		{"Delete", func(m *skiplistmap.Map, key string) bool { return m.Delete(key) }},
		{"Purge", func(m *skiplistmap.Map, key string) bool { return m.Purge(key) }},
	}
}

// rangeKeys returns the keys Range visits and fails when one is visited twice.
func rangeKeys(t *testing.T, m *skiplistmap.Map) map[string]bool {
	t.Helper()
	seen := map[string]bool{}
	m.Range(func(k, v interface{}) bool {
		if seen[k.(string)] {
			t.Errorf("Range visited %q twice", k)
		}
		seen[k.(string)] = true
		return true
	})
	return seen
}

func rangeCount(t *testing.T, m *skiplistmap.Map) int {
	return len(rangeKeys(t, m))
}

// An empty map answers Get/Delete/Range without panic and reports Len 0.
func Test_EmptyMap(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			if v, ok := m.base.Get("missing"); ok || v != nil {
				t.Errorf("Get on empty map = (%v, %v), want (nil, false)", v, ok)
			}
			if m.base.Delete("missing") {
				t.Errorf("Delete on empty map returned true")
			}
			if m.base.Purge("missing") {
				t.Errorf("Purge on empty map returned true")
			}
			if got := m.base.Len(); got != 0 {
				t.Errorf("Len() = %d, want 0", got)
			}
			if got := rangeCount(t, m.base); got != 0 {
				t.Errorf("Range visited %d keys, want 0", got)
			}
		})
	}
}

// Delete and Purge remove one key: Len and Range shrink by one, Get misses,
// a second delete is false and does not change Len, a missing key is false.
func Test_DeleteLen(t *testing.T) {
	for _, p := range crashMapParams() {
		for _, d := range seqDeletes() {
			t.Run(p.name+"/"+d.name, func(t *testing.T) {
				m := p.newMap()
				prefill(t, m, seqCnt)
				if got := m.base.Len(); got != seqCnt {
					t.Fatalf("Len() after prefill = %d, want %d", got, seqCnt)
				}
				key := crashKey(seqCnt / 2)
				if !d.fn(m.base, key) {
					t.Fatalf("%s(%q) returned false", d.name, key)
				}
				if got := m.base.Len(); got != seqCnt-1 {
					t.Errorf("Len() after %s = %d, want %d", d.name, got, seqCnt-1)
				}
				if _, ok := m.base.Get(key); ok {
					t.Errorf("Get(%q) found after %s", key, d.name)
				}
				seen := rangeKeys(t, m.base)
				if len(seen) != seqCnt-1 || seen[key] {
					t.Errorf("Range visited %d keys after %s (deleted key seen: %v), want %d", len(seen), d.name, seen[key], seqCnt-1)
				}
				if d.fn(m.base, key) {
					t.Errorf("second %s(%q) returned true", d.name, key)
				}
				if d.fn(m.base, "missing") {
					t.Errorf("%s of a missing key returned true", d.name)
				}
				if got := m.base.Len(); got != seqCnt-1 {
					t.Errorf("Len() after repeated %s = %d, want %d", d.name, got, seqCnt-1)
				}
			})
		}
	}
}

// After Delete or Purge, Set of the same key stores the new value; Get returns
// it, Len and Range are back to the full count, and the other keys are intact.
func Test_DeleteReinsertValue(t *testing.T) {
	for _, p := range crashMapParams() {
		for _, d := range seqDeletes() {
			t.Run(p.name+"/"+d.name, func(t *testing.T) {
				m := p.newMap()
				prefill(t, m, seqCnt)
				for i := 0; i < seqCnt; i += 7 {
					key := crashKey(i)
					if !d.fn(m.base, key) {
						t.Fatalf("%s(%q) returned false", d.name, key)
					}
				}
				for i := 0; i < seqCnt; i += 7 {
					key := crashKey(i)
					want := &list_head.ListHead{}
					if !m.Set(key, want) {
						t.Fatalf("Set(%q) after %s returned false", key, d.name)
					}
					got, ok := m.Get(key)
					if !ok || got != want {
						t.Errorf("Get(%q) after re-insert = (%p, %v), want (%p, true)", key, got, ok, want)
					}
				}
				if got := m.base.Len(); got != seqCnt {
					t.Errorf("Len() after re-insert = %d, want %d", got, seqCnt)
				}
				if got := rangeCount(t, m.base); got != seqCnt {
					t.Errorf("Range visited %d keys after re-insert, want %d", got, seqCnt)
				}
				assertAllFound(t, m, seqCnt)

				// delete the re-inserted entries again
				for i := 0; i < seqCnt; i += 7 {
					key := crashKey(i)
					if !d.fn(m.base, key) {
						t.Fatalf("second %s(%q) after re-insert returned false", d.name, key)
					}
					if _, ok := m.base.Get(key); ok {
						t.Errorf("Get(%q) found after second %s", key, d.name)
					}
				}
				deleted := (seqCnt + 6) / 7
				if got := m.base.Len(); got != seqCnt-deleted {
					t.Errorf("Len() after second %s = %d, want %d", d.name, got, seqCnt-deleted)
				}
				seen := rangeKeys(t, m.base)
				if len(seen) != seqCnt-deleted || seen[crashKey(0)] {
					t.Errorf("Range visited %d keys after second %s (deleted key seen: %v), want %d", len(seen), d.name, seen[crashKey(0)], seqCnt-deleted)
				}
			})
		}
	}
}

// sameBucketKeys returns n keys whose hashes share the top 4 bits of the
// reverse, so a small map keeps them in one bucket, sorted by reverse.
// The hash seed differs per process, so the keys are picked at run time.
func sameBucketKeys(n int) []string {
	byNibble := map[uint64][]string{}
	for i := 0; ; i++ {
		k := fmt.Sprintf("s%d", i)
		nib := keyReverse(k) >> 60
		byNibble[nib] = append(byNibble[nib], k)
		if keys := byNibble[nib]; len(keys) == n {
			sortByReverse(keys)
			return keys
		}
	}
}

func keyReverse(k string) uint64 {
	h, _ := skiplistmap.KeyToHash(k)
	return bits.Reverse64(h)
}

func sortByReverse(keys []string) {
	sort.Slice(keys, func(a, b int) bool { return keyReverse(keys[a]) < keyReverse(keys[b]) })
}

// splitBucketKeys returns n keys below and n keys above the reverse where one
// top-level bucket splits first: halfway to the next bucket, as makeBucket2
// computes it. Each side is sorted by reverse.
func splitBucketKeys(n int) (below, above []string) {
	sides := map[uint64]*[2][]string{}
	for i := 0; ; i++ {
		k := fmt.Sprintf("s%d", i)
		r := keyReverse(k)
		nib := r >> 60
		if sides[nib] == nil {
			sides[nib] = &[2][]string{}
		}
		s := sides[nib]
		side := 0
		if r >= nib<<60+1<<59 {
			side = 1
		}
		s[side] = append(s[side], k)
		if len(s[0]) >= n && len(s[1]) >= n {
			below, above = s[0][:n], s[1][:n]
			sortByReverse(below)
			sortByReverse(above)
			return
		}
	}
}

// Sequences that broke an embedded pool, shrunk from random runs. rN is the
// N-th key of one bucket; BN and AN are the N-th keys below and above the
// point where one bucket splits first (with bucket=16 the 17th item splits).
func Test_PoolSequences(t *testing.T) {
	small := sameBucketKeys(6)
	below, above := splitBucketKeys(30)
	for _, c := range []struct {
		name string
		ops  []string
	}{
		{"reinsert below the only deleted key", []string{"Set r2", "Del r2", "Set r0", "Set r0", "Set r2"}},
		{"reinsert below the only purged key", []string{"Set r2", "Pur r2", "Set r0", "Set r0", "Set r2"}},
		{"reinsert below a deleted first key", []string{"Set r1", "Set r2", "Del r1", "Set r0", "Set r0", "Set r1"}},
		{"reinsert below a purged first key", []string{"Set r1", "Set r2", "Pur r1", "Set r0", "Set r0", "Set r1"}},
		{"insert above a deleted last item", []string{"Set r4", "Set r1", "Del r4", "Set r3", "Set r5", "Set r4"}},
		{"insert below a deleted last item", []string{"Set r1", "Set r3", "Del r3", "Set r2"}},
		{"split at a deleted item", []string{"Set B17", "Set A1", "Set A12", "Set B7", "Set A4", "Set B2", "Set B5", "Set B9", "Set B16", "Set A6", "Del A1", "Set B6", "Set A21", "Set B8", "Set B14", "Set A17", "Set B11", "Set B13", "Set B10", "Set A11"}},
		{"insert after a purged first key", []string{"Set r0", "Set r1", "Pur r0", "Set r3", "Set r2", "Set r4"}},
		{"expand with a purged first item", []string{"Set A7", "Set B5", "Set B12", "Set A8", "Set A6", "Set A18", "Set A16", "Set B14", "Set B6", "Set A13", "Set B10", "Set A9", "Set A3", "Set A0", "Set B7", "Set B13", "Set B3", "Pur B3", "Set B15"}},
		{"expand with a purged last item", []string{"Set B0", "Set B2", "Set B4", "Set B6", "Set B8", "Set B10", "Set B12", "Set B14", "Set B16", "Set B18", "Set B20", "Set B22", "Set B24", "Set B26", "Set B28", "Set A0", "Pur B28", "Set A1", "Set B1"}},
	} {
		for _, p := range crashMapParams() {
			t.Run(c.name+"/"+p.name, func(t *testing.T) {
				m := p.newMap()
				want := map[string]*list_head.ListHead{}
				for step, op := range c.ops {
					var verb string
					var side rune
					var n int
					fmt.Sscanf(op, "%s %c%d", &verb, &side, &n)
					k := map[rune][]string{'r': small, 'B': below, 'A': above}[side][n]
					switch verb {
					case "Set":
						v := &list_head.ListHead{}
						m.Set(k, v)
						want[k] = v
					case "Del", "Pur":
						d := m.base.Delete
						if verb == "Pur" {
							d = m.base.Purge
						}
						if _, live := want[k]; d(k) != live {
							t.Errorf("step %d %s: returned %v", step, op, !live)
						}
						delete(want, k)
					}
					for wk, wv := range want {
						if v, ok := m.Get(wk); !ok || v != wv {
							t.Fatalf("step %d %s: Get(%q) = (%p, %v), want (%p, true)", step, op, wk, v, ok, wv)
						}
					}
					if got := m.base.Len(); got != len(want) {
						t.Fatalf("step %d %s: Len() = %d, want %d", step, op, got, len(want))
					}
					if got := rangeCount(t, m.base); got != len(want) {
						t.Fatalf("step %d %s: Range visited %d keys, want %d", step, op, got, len(want))
					}
				}
			})
		}
	}
}

// Random Set, Delete and Purge steps on the keys of one bucket, checked
// against a Go map after every step. With one bucket the pool splits,
// expands and reuses deleted items.
func Test_SequentialModel(t *testing.T) {
	keys := sameBucketKeys(120)
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			for seed := int64(1); seed <= 10; seed++ {
				r := rand.New(rand.NewSource(seed))
				m := p.newMap()
				want := map[string]*list_head.ListHead{}
				for step := 0; step < 1500; step++ {
					k := keys[r.Intn(len(keys))]
					switch op := r.Intn(6); {
					case op < 3:
						v := &list_head.ListHead{}
						m.Set(k, v)
						want[k] = v
					case op < 5:
						d := seqDeletes()[op-3]
						_, live := want[k]
						if got := d.fn(m.base, k); got != live {
							t.Fatalf("seed %d step %d: %s(%q) = %v, want %v", seed, step, d.name, k, got, live)
						}
						delete(want, k)
					default:
						if got := rangeCount(t, m.base); got != len(want) {
							t.Fatalf("seed %d step %d: Range visited %d keys, want %d", seed, step, got, len(want))
						}
					}
					wv, live := want[k]
					if v, ok := m.Get(k); ok != live || v != wv {
						t.Fatalf("seed %d step %d: Get(%q) = (%p, %v), want (%p, %v)", seed, step, k, v, ok, wv, live)
					}
					if got := m.base.Len(); got != len(want) {
						t.Fatalf("seed %d step %d: Len() = %d, want %d", seed, step, got, len(want))
					}
				}
			}
		})
	}
}
