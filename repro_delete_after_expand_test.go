//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_DeleteWaitsForTheRepairOfTheNewPool replays this order:
//
//  1. The main goroutine sets 64 adjacent keys, which fill the pool.
//  2. G1 sets the 65th key: _expand copies the items into a new array,
//     repairs the links and stops at pool.expand.repaired. Lookups find the
//     items in the new array now.
//  3. G2 deletes k and finds the item of k in the new array. It must hold the
//     new pool while it marks the item, and so wait until the expand lets
//     writers of the new pool count: an expand of the new pool that starts
//     later must not copy the item while G2 marks it.
func Test_DeleteWaitsForTheRepairOfTheNewPool(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]

	s := newStepper(t)
	repaired := s.stopAt("map.pool.expand.repaired", nil)
	done1 := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	repaired.waitReached(t, done1)
	done2 := goStep(t, func() { m.base.Delete(k) })
	if waitAtMost(done2, 200*time.Millisecond) {
		t.Errorf("Delete(k) marked the item of k before the new pool let writers count")
	}
	repaired.Release()
	waitDone(t, done1, "Set of the 65th key")
	waitDone(t, done2, "Delete(k)")

	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found k after Delete(k)")
	}
	if got := m.base.Len(); got != n {
		t.Errorf("Len() = %d, want %d", got, n)
	}
}

// Test_DeleteOfAMovedItemOfAKeyDeletedMeanwhile replays this order:
//
//  1. The main goroutine sets 64 adjacent keys, which fill the pool.
//  2. G1: Delete(k) finds the item of k in the array of the pool and stops
//     at delete.found.
//  3. The main goroutine sets the 65th key: _expand copies the items into a
//     new array and ends. The item that G1 found is in the old array, which
//     no pool holds any more.
//  4. The main goroutine deletes k: Delete marks the item of k in the new
//     array.
//  5. G1 resumes: no pool holds its item, and k is not found again. k was
//     deleted by the Delete of step 4, so G1 must not delete it once more:
//     it must return false and not lower the length again.
func Test_DeleteOfAMovedItemOfAKeyDeletedMeanwhile(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]

	s := newStepper(t)
	found := s.stopAt("map.delete.found", nil)
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m.base.Delete(k) })
	found.waitReached(t, done1)
	setKeys(t, m, keys[n:])
	if !m.base.Delete(k) {
		t.Fatalf("Delete(k) after the expand did not find k")
	}
	found.Release()
	waitDone(t, done1, "the first Delete(k)")

	if ok1 {
		t.Errorf("the first Delete(k) returned true, but the second one deleted k")
	}
	if got := m.base.Len(); got != n {
		t.Errorf("Len() = %d, want %d", got, n)
	}
}
