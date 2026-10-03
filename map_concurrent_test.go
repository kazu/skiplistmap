package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

func newDefaultMap() *WrapHMap {
	return newWrapHMap(skiplistmap.New[skiplistmap.StringKey, any]())
}

// poolMapParams are the configurations without the embedded pool: Set takes
// items from the Map's item pool, and StoreItem links the caller's items.
func poolMapParams() []crashMapParam {
	return []crashMapParam{
		{"skiplistmap4 bucket=16", func() *WrapHMap { return newPoolMap(16) }},
		{"skiplistmap4 bucket=32", func() *WrapHMap { return newPoolMap(32) }},
		{"default", newDefaultMap},
	}
}

// runTogether starts fn(g) for g in [0, n) at the same time and waits for all.
func runTogether(n int, fn func(g int)) {
	var start, wg sync.WaitGroup
	start.Add(1)
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			start.Wait()
			fn(g)
		}(g)
	}
	start.Done()
	wg.Wait()
}

// keysWithDistinctTopBits returns n keys whose reversed hashes differ in
// the top 4 bits modulo 8. Their items go to different top-level buckets and
// are taken from different sub-pools of the Map's item pool.
func keysWithDistinctTopBits(n int) []string {
	const subPools = 8
	seen := map[uint64]bool{}
	var keys []string
	for i := 0; len(keys) < n; i++ {
		k := crashKey(i)
		idx := (bits.Reverse64(skiplistmap.MemHashString(k)) >> 60) % subPools
		if seen[idx] {
			continue
		}
		seen[idx] = true
		keys = append(keys, k)
	}
	return keys
}

// Items of keys in different top-level buckets are linked by StoreItem at
// the same time on a new Map: every key must be found.
func Test_ConcurrentFirstStoreItem(t *testing.T) {
	const goroutines = 8
	keys := keysWithDistinctTopBits(goroutines)
	for _, p := range poolMapParams() {
		t.Run(p.name, func(t *testing.T) {
			for round := 0; round < 200; round++ {
				m := p.newMap()
				items := make([]skiplistmap.SampleItem[skiplistmap.StringKey, any], goroutines)
				for g := range items {
					items[g].InitEntry(skiplistmap.StringKey(keys[g]), &list_head.ListHead{})
				}
				runWithDeadline(t, time.Minute, func() {
					runTogether(goroutines, func(g int) {
						m.base.StoreItem(&items[g])
					})
				})
				for _, k := range keys {
					if _, ok := m.Get(k); !ok {
						t.Fatalf("round %d: Get(%q) not found", round, k)
					}
				}
				runtime.KeepAlive(items)
			}
		})
	}
}

// The first Sets of a new Map run at the same time: the Map must make one
// item pool, and every item must stay reachable after a GC.
func Test_ConcurrentFirstSet(t *testing.T) {
	const goroutines = 8
	keys := keysWithDistinctTopBits(goroutines)
	for round := 0; round < 200; round++ {
		m := newDefaultMap()
		runWithDeadline(t, time.Minute, func() {
			runTogether(goroutines, func(g int) {
				m.Set(keys[g], &list_head.ListHead{})
			})
		})
		runtime.GC()
		for _, k := range keys {
			if _, ok := m.Get(k); !ok {
				t.Fatalf("round %d: Get(%q) not found", round, k)
			}
		}
	}
}

// Lookups of absent keys run at the same time. A miss records its key in
// Failreverse only while Failreverse is 0, so the test clears it first.
func Test_ConcurrentGetMissing(t *testing.T) {
	params := append(crashMapParams(), crashMapParam{"default", newDefaultMap})
	for _, p := range params {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			prefill(t, m, 1000)
			skiplistmap.Failreverse = 0
			runWithDeadline(t, time.Minute, func() {
				runTogether(8, func(g int) {
					for i := 0; i < 1000; i++ {
						if _, ok := m.Get(fmt.Sprintf("missing-%d-%d", g, i)); ok {
							t.Errorf("Get(missing-%d-%d) found", g, i)
							return
						}
					}
				})
			})
		})
	}
}

// One goroutine links new items with StoreItem, which splits buckets, while
// others look up keys stored before: no lookup may miss. The items are the
// test's, so no item pool grows.
func Test_SearchDuringSplit(t *testing.T) {
	for _, p := range poolMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const stored = 1000
			const added = 20000
			m := p.newMap()
			items := make([]skiplistmap.SampleItem[skiplistmap.StringKey, any], stored+added)
			for i := range items {
				items[i].InitEntry(skiplistmap.StringKey(crashKey(i)), &list_head.ListHead{})
			}
			for i := 0; i < stored; i++ {
				m.base.StoreItem(&items[i])
			}
			var stop atomic.Bool
			var misses atomic.Int64
			runWithDeadline(t, 2*time.Minute, func() {
				var wg sync.WaitGroup
				for g := 0; g < 8; g++ {
					wg.Add(1)
					go func(g int) {
						defer wg.Done()
						for i := g; !stop.Load(); i++ {
							if _, ok := m.Get(crashKey(i % stored)); !ok {
								misses.Add(1)
							}
						}
					}(g)
				}
				for i := stored; i < stored+added; i++ {
					m.base.StoreItem(&items[i])
					if i%997 == 0 {
						runtime.GC()
					}
				}
				stop.Store(true)
				wg.Wait()
			})
			if n := misses.Load(); n > 0 {
				t.Errorf("%d lookups of stored keys missed", n)
			}
			assertAllFound(t, m, stored+added)
			runtime.KeepAlive(items)
		})
	}
}

// Goroutines add different new keys to a map with the embedded pool at the
// same time: every key must be found once, and setting it again must not add
// a second item.
func Test_ConcurrentNewKeys(t *testing.T) {
	for _, p := range crashMapParams()[:2] {
		t.Run(p.name, func(t *testing.T) {
			const goroutines = 16
			const perGoroutine = 5000
			const cnt = goroutines * perGoroutine
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				runTogether(goroutines, func(g int) {
					for i := 0; i < perGoroutine; i++ {
						m.Set(crashKey(g*perGoroutine+i), &list_head.ListHead{})
					}
				})
			})
			runtime.GC()
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, cnt)
			})
			if got := m.base.Len(); got != cnt {
				t.Errorf("Len() = %d, want %d", got, cnt)
			}
			runWithDeadline(t, 2*time.Minute, func() {
				prefill(t, m, cnt)
			})
			if got := m.base.Len(); got != cnt {
				t.Errorf("Len() after setting every key again = %d, want %d", got, cnt)
			}
		})
	}
}

// Goroutines link different new items with StoreItem at the same time: every
// key must be found. The items are kept alive by the test, as StoreItem requires.
func Test_ConcurrentStoreItem(t *testing.T) {
	for _, p := range poolMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const goroutines = 16
			const perGoroutine = 2000
			const cnt = goroutines * perGoroutine
			m := p.newMap()
			items := make([]skiplistmap.SampleItem[skiplistmap.StringKey, any], cnt)
			for i := range items {
				items[i].InitEntry(skiplistmap.StringKey(crashKey(i)), &list_head.ListHead{})
			}
			runWithDeadline(t, 2*time.Minute, func() {
				runTogether(goroutines, func(g int) {
					for i := g * perGoroutine; i < (g+1)*perGoroutine; i++ {
						m.base.StoreItem(&items[i])
					}
				})
			})
			runtime.GC()
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, cnt)
			})
			if got := m.base.Len(); got != cnt {
				t.Errorf("Len() = %d, want %d", got, cnt)
			}
			runtime.KeepAlive(items)
		})
	}
}
