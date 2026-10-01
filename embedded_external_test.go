package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"testing"

	"github.com/kazu/skiplistmap"
)

type embeddedExternalKey struct {
	id       int
	reverse  uint64
	conflict uint64
}

func (k embeddedExternalKey) KeyHash() (uint64, uint64) {
	return bits.Reverse64(k.reverse), k.conflict
}

func (k embeddedExternalKey) Equal(other embeddedExternalKey) bool { return k.id == other.id }

func TestEmbeddedExternalEntries(t *testing.T) {
	for _, layout := range []string{"external", "mixed", "collision"} {
		t.Run(layout, func(t *testing.T) {
			m := skiplistmap.New[embeddedExternalKey, int](
				skiplistmap.UseEmbeddedPool[embeddedExternalKey, int](true),
				skiplistmap.MaxPefBucket[embeddedExternalKey, int](8))
			const n = 96
			keys := make([]embeddedExternalKey, n)
			entries := make([]*skiplistmap.Entry[embeddedExternalKey, int], n)
			defer runtime.KeepAlive(entries)
			for i := range keys {
				keys[i] = embeddedExternalKey{i, uint64((i*37)%n) << 56, uint64(i % 3)}
				if layout == "collision" {
					keys[i].reverse, keys[i].conflict = 1<<61, 7
				}
				if layout == "external" || i%2 == 0 {
					entries[i] = skiplistmap.NewEntry(keys[i], i)
					if !m.StoreItem(entries[i]) {
						t.Fatalf("StoreItem(%d)", i)
					}
				} else if !m.Set(keys[i], i) {
					t.Fatalf("Set(%d)", i)
				}
				for j := 0; j <= i; j++ {
					got, ok := m.LoadItem(keys[j])
					if !ok || got.Value() != j || entries[j] != nil && got != entries[j] {
						t.Fatalf("after insert %d: LoadItem(%d)=(%p,%v), external=%p", i, j, got, ok, entries[j])
					}
				}
				if got := m.Len(); got != i+1 {
					t.Fatalf("after insert %d: Len=%d", i, got)
				}
			}
			for i, key := range keys {
				if entries[i] == nil {
					item, _ := m.LoadItem(key)
					if m.StoreItem(item) {
						t.Fatalf("StoreItem accepted pool item %d", i)
					}
				}
			}
			for i, key := range keys {
				if !m.Set(key, i+n) {
					t.Fatalf("update(%d)", i)
				}
				if got, ok := m.Get(key); !ok || got != i+n {
					t.Fatalf("updated Get(%d)=(%d,%v)", i, got, ok)
				}
				if entries[i] != nil && entries[i].Value() != i {
					t.Fatalf("update changed the original snapshot for %d", i)
				}
			}
			seen := make(map[int]int)
			m.Range(func(k embeddedExternalKey, v int) bool { seen[k.id] = v; return true })
			if len(seen) != n || m.Len() != n {
				t.Fatalf("after updates: Range=%d Len=%d", len(seen), m.Len())
			}
			for i, key := range keys {
				if seen[i] != i+n {
					t.Fatalf("Range(%d)=%d", i, seen[i])
				}
				if i%2 == 0 {
					if !m.Delete(key) {
						t.Fatalf("Delete(%d)", i)
					}
				} else if !m.Purge(key) {
					t.Fatalf("Purge(%d)", i)
				}
				if _, ok := m.Get(key); ok {
					t.Fatalf("deleted key %d remains", i)
				}
				if !m.Set(key, -i) {
					t.Fatalf("reinsert(%d)", i)
				}
				if got, ok := m.Get(key); !ok || got != -i {
					t.Fatalf("reinserted Get(%d)=(%d,%v)", i, got, ok)
				}
			}
			if m.Len() != n {
				t.Fatalf("after reuse: Len=%d", m.Len())
			}
		})
	}
}

func TestEmbeddedExternalStoreSameKey(t *testing.T) {
	m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](true))
	entries := []*skiplistmap.Entry[skiplistmap.IntKey, int]{
		skiplistmap.NewEntry[skiplistmap.IntKey, int](7, 1),
		skiplistmap.NewEntry[skiplistmap.IntKey, int](7, 2),
	}
	defer runtime.KeepAlive(entries)
	start := make(chan struct{})
	var writers sync.WaitGroup
	for _, entry := range entries {
		writers.Add(1)
		go func(entry *skiplistmap.Entry[skiplistmap.IntKey, int]) {
			defer writers.Done()
			<-start
			if !m.StoreItem(entry) {
				t.Error("StoreItem")
			}
		}(entry)
	}
	close(start)
	writers.Wait()
	if value, ok := m.Get(7); !ok || value < 1 || value > 2 || m.Len() != 1 {
		t.Fatalf("Get=(%d,%v), Len=%d", value, ok, m.Len())
	}
	if !m.Set(7, 3) {
		t.Fatal("Set")
	}
	if value, ok := m.Get(7); !ok || value != 3 || m.Len() != 1 {
		t.Fatalf("updated Get=(%d,%v), Len=%d", value, ok, m.Len())
	}
}

func ExampleMap_StoreItem_embedded() {
	type record struct {
		entry skiplistmap.Entry[skiplistmap.IntKey, string]
		note  string
	}
	owner := &record{note: "caller owned"}
	owner.entry.InitEntry(1, "external")
	m := skiplistmap.New[skiplistmap.IntKey, string](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, string](true))
	m.StoreItem(&owner.entry)
	m.Set(2, "pooled")
	got, ok := m.LoadItem(1)
	fmt.Println(ok, got == &owner.entry, owner.note)
	fmt.Println(m.Get(2))
	runtime.KeepAlive(owner)
	// Output:
	// true true caller owned
	// pooled true
}

func TestEmbeddedExternalBoundaryHashes(t *testing.T) {
	for _, reverse := range []uint64{0, 1, 1 << 63, ^uint64(0)} {
		t.Run(fmt.Sprintf("%x", reverse), func(t *testing.T) {
			m := skiplistmap.New[embeddedExternalKey, int](skiplistmap.UseEmbeddedPool[embeddedExternalKey, int](true))
			key := embeddedExternalKey{1, reverse, 0}
			e := skiplistmap.NewEntry(key, 7)
			defer runtime.KeepAlive(e)
			if !m.StoreItem(e) {
				t.Fatal("StoreItem")
			}
			if got, ok := m.LoadItem(key); !ok || got != e {
				t.Fatalf("LoadItem=(%p,%v), want %p", got, ok, e)
			}
			if !m.Purge(key) || m.Len() != 0 {
				t.Fatal("Purge")
			}
			if !m.StoreItem(e) {
				t.Fatal("StoreItem after Purge")
			}
			if got, ok := m.LoadItem(key); !ok || got != e {
				t.Fatalf("reinserted LoadItem=(%p,%v), want %p", got, ok, e)
			}
		})
	}
}

func TestEmbeddedExternalConcurrent(t *testing.T) {
	m := skiplistmap.New[skiplistmap.IntKey, int](
		skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](true),
		skiplistmap.MaxPefBucket[skiplistmap.IntKey, int](8))
	const n = 48
	entries := make([]*skiplistmap.Entry[skiplistmap.IntKey, int], n)
	defer runtime.KeepAlive(entries)
	var writers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for i := worker; i < n; i += 4 {
				key := skiplistmap.IntKey(i)
				entries[i] = skiplistmap.NewEntry(key, i)
				if !m.StoreItem(entries[i]) {
					t.Errorf("StoreItem(%d)", i)
					return
				}
				for j := 0; j < 10; j++ {
					if !m.Set(key, j) {
						t.Errorf("Set(%d)", i)
						return
					}
					if value, ok := m.Get(key); !ok || value != j {
						t.Errorf("Get(%d)=(%d,%v), want %d", i, value, ok, j)
						return
					}
				}
				if !m.Purge(key) || !m.Set(key, i) {
					t.Errorf("Purge/Set(%d)", i)
					return
				}
			}
		}(worker)
	}
	writers.Wait()
	if m.Len() != n {
		t.Fatalf("Len=%d, want %d", m.Len(), n)
	}
	for i := 0; i < n; i++ {
		if value, ok := m.Get(skiplistmap.IntKey(i)); !ok || value != i {
			t.Fatalf("final Get(%d)=(%d,%v)", i, value, ok)
		}
	}
}
