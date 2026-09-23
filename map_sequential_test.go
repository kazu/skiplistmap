package skiplistmap_test

import (
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
