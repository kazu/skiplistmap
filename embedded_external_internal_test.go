package skiplistmap

import (
	"math/bits"
	"runtime"
	"testing"
)

func TestEmbeddedExternalPoolMovesAndSplits(t *testing.T) {
	m := New[Uint64Key, int](UseEmbeddedPool[Uint64Key, int](true), MaxPefBucket[Uint64Key, int](4))
	const n = 32
	keys := make([]Uint64Key, n)
	entries := make([]*Entry[Uint64Key, int], n)
	defer runtime.KeepAlive(entries)
	var first *Entry[Uint64Key, int]
	for i := range keys {
		reverse := uint64(1<<61) + uint64(i)<<55
		keys[i] = Uint64Key(bits.Reverse64(reverse))
		if i%2 == 0 {
			if !m.Set(keys[i], i) {
				t.Fatalf("Set(%d)", i)
			}
			if i == 0 {
				first, _ = m.LoadItem(keys[i])
			}
		} else {
			entries[i] = NewEntry(keys[i], i)
			if !m.StoreItem(entries[i]) {
				t.Fatalf("StoreItem(%d)", i)
			}
		}
	}
	owners := make(map[*bucket[Uint64Key, int]]bool)
	for i, key := range keys {
		item, ok := m.LoadItem(key)
		if !ok || item.Value() != i || entries[i] != nil && item != entries[i] {
			t.Fatalf("LoadItem(%d)=(%p,%v), external=%p", i, item, ok, entries[i])
		}
		owners[m.findBucket(bits.Reverse64(uint64(key))).toBase()] = true
	}
	if current, _ := m.LoadItem(keys[0]); current == first {
		t.Fatal("fixture did not move the first pool entry")
	}
	if len(owners) < 2 {
		t.Fatal("fixture did not split the initial bucket")
	}
	for i, key := range keys {
		if !m.Purge(key) {
			t.Fatalf("Purge(%d)", i)
		}
		if entries[i] != nil {
			if m.StoreItem(entries[i]) {
				t.Fatalf("retired StoreItem after split/Purge(%d)", i)
			}
			fresh := entries[i].Copy()
			defer runtime.KeepAlive(fresh)
			if !m.StoreItem(fresh) {
				t.Fatalf("StoreItem after split/Purge(%d)", i)
			}
		} else if !m.Set(key, i) {
			t.Fatalf("Set after split/Purge(%d)", i)
		}
	}
	if m.Len() != n {
		t.Fatalf("Len=%d, want %d", m.Len(), n)
	}
}
