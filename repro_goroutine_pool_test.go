//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_GoroutinePoolExpandWaitsForAnItemBeingLinked replays this order with
// UseGoroutineInPool:
//
//  1. The main goroutine sets 63 adjacent keys into a pool of 64 items.
//  2. G1 sets the 64th key: the goroutine of the pool hands it the last item,
//     and G1 stops at map.set.beforeInit, before it links the item.
//  3. G2 sets the 65th key: the goroutine of the pool runs _expand.
//  4. _expand must not copy the items before G1 has linked its item, as for
//     Pool.Get: the copy would miss the link of G1.
func Test_GoroutinePoolExpandWaitsForAnItemBeingLinked(t *testing.T) {
	skiplistmap.UseGoroutineInPool = true
	t.Cleanup(func() { skiplistmap.UseGoroutineInPool = false })
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n-1])

	s := newStepper(t)
	beforeInit := s.stopAt("map.set.beforeInit", nil)
	done1 := goStep(t, func() { m.Set(keys[n-1], &list_head.ListHead{}) })
	beforeInit.waitReached(t, done1)
	done2 := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	waitAtMost(done2, 200*time.Millisecond)
	if s.total("map.pool.expand.linked") != 0 {
		t.Errorf("_expand went past the wait for the links while G1 was linking its item")
	}
	beforeInit.Release()
	waitDone(t, done1, "Set of the 64th key")
	waitDone(t, done2, "Set of the 65th key")

	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
}
