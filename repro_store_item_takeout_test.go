//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_StoreItemTakingOutWaitsForAnExpandOfAnotherMap replays this order:
//
//  1. m2 holds x, an item not from a pool, next to the 64 items of a full
//     pool, and Delete(x) leaves x deleted and linked in m2.
//  2. G1 sets the 65th key of m2: _expand copies the items and stops at
//     pool.expand.copied, before it repairs the links.
//  3. G2 stores x into m1. StoreItem takes x out of the list of m2 before
//     it links x, which changes the links of the items of the pool of m2
//     next to x. It must not do so while that pool is being expanded.
func Test_StoreItemTakingOutWaitsForAnExpandOfAnotherMap(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 2)
	x := newStepItems(keys[n+1:])
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m2, keys[:n])
	if !m2.base.StoreItem(&x[0]) {
		t.Fatalf("m2.StoreItem(x) failed")
	}
	if !m2.base.Delete(keys[n+1]) {
		t.Fatalf("m2.Delete(x) failed")
	}

	s := newStepper(t)
	copied := s.stopAt("map.pool.expand.copied", nil)
	done1 := goStep(t, func() { m2.Set(keys[n], &list_head.ListHead{}) })
	copied.waitReached(t, done1)
	done2 := goStep(t, func() { m1.base.StoreItem(&x[0]) })
	waitAtMost(done2, 200*time.Millisecond)
	if n := s.count("map.set.beforeInit", nodeOf(&x[0])); n != 0 {
		t.Errorf("m1.StoreItem(x) went to take x out while the pool of m2 was being expanded")
	}
	copied.Release()
	waitDone(t, done1, "Set of the 65th key of m2")
	waitDone(t, done2, "m1.StoreItem(x)")

	if err := skiplistmap.StepCheckLists(m2.base); err != nil {
		t.Errorf("list of m2 broken: %v", err)
	}
	if _, ok := m1.Get(keys[n+1]); !ok {
		t.Errorf("m1.Get(x) not found")
	}
	for _, k := range keys[:n+1] {
		if _, ok := m2.Get(k); !ok {
			t.Errorf("m2.Get(%q) not found", k)
		}
	}
	runtime.KeepAlive(x)
}
