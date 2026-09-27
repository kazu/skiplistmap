//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_GoroutinePoolGetRetriesAFailedExpand replays this order with
// UseGoroutineInPool, where one goroutine per pool list runs every Get:
//
//  1. The main goroutine sets 64 adjacent keys, which fill the pool.
//  2. G1 sets the 65th key. The goroutine of the pool runs _expand, which
//     copies the items and stops at elist.repair.prevLinked, after it moved
//     the link from the node before the first item to the copy.
//  3. The main goroutine stores z, a key after the 65 keys that does not come
//     from the pool, next to the last item of the old array. The repair can
//     no longer move the prev link of the node after that item to the copy,
//     and fails.
//  4. The Get of G1 must take an item from the pool again, not hand G1 no
//     item.
func Test_GoroutinePoolGetRetriesAFailedExpand(t *testing.T) {
	skiplistmap.UseGoroutineInPool = true
	t.Cleanup(func() { skiplistmap.UseGoroutineInPool = false })
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 2)
	z := newStepItems(keys[n+1:])
	m := newStepMap()
	setKeys(t, m, keys[:n])

	s := newStepper(t)
	st := s.stopAt("elist.repair.prevLinked", nil)
	done := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	st.waitReached(t, done)
	doneZ := goStep(t, func() { m.base.StoreItem(&z[0]) })
	waitAtMost(doneZ, time.Second)
	st.Release()
	waitDone(t, done, "Set of the 65th key")
	waitDone(t, doneZ, "StoreItem(z)")

	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
	runtime.KeepAlive(z)
}
