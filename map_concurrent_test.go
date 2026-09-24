package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

func newDefaultMap() *WrapHMap {
	return newWrapHMap(skiplistmap.New())
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
				items := make([]skiplistmap.SampleItem, goroutines)
				for g := range items {
					items[g].K = keys[g]
					items[g].SetValue(&list_head.ListHead{})
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
