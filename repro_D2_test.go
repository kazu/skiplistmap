//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// D2 (skiplistmap4): two Delete of one key k across an expand of the pool.
// G0 sets the 65th key and stops after the expand copied the items, k in the
// old array O to C in the new one. GA deletes k: it finds O, claims it, and
// waits for the expand to end before it deletes C too. G0 ends the expand,
// and GB deletes k: it finds C, which the copy made before the claim of GA,
// and claims O, the origin of C. One of the two Delete must return false, and
// Len must go down by one.
func Test_D2DeletesAcrossAnExpandBothReturnTrue(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	o := nodeOf(item.(*skiplistmap.SampleItem))

	s := newStepper(t)
	expand := s.stopAt("map.pool.expand.copied", nil)
	done0 := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	expand.waitReached(t, done0)
	stA := s.stopAt("elist.move.waitDone", isNode(o))
	var okA bool
	doneA := goStep(t, func() { okA = m.base.Delete(k) })
	stA.waitReached(t, doneA)
	expand.Release()
	waitDone(t, done0, "Set of the 65th key")
	okB := m.base.Delete(k)
	// GB lost to GA, which has not deleted C yet: GB deleted C before it
	// returned
	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found after Delete(k) of GB returned")
	}
	stA.Release()
	waitDone(t, doneA, "Delete(k) of GA")

	if okA == okB {
		t.Errorf("Delete(k) of GA = %v and of GB = %v, want one true", okA, okB)
	}
	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found after Delete(k)")
	}
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}

// D2 the other way: GA finds k in the old array O and stops before it claims
// it, while the expand ends and GB deletes k from its copy C. GA must return
// false, and must leave k deleted.
func Test_D2DeleteOfTheCopyGetsAheadOfTheOldItem(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	o := nodeOf(item.(*skiplistmap.SampleItem))

	s := newStepper(t)
	expand := s.stopAt("map.pool.expand.copied", nil)
	done0 := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	expand.waitReached(t, done0)
	stA := s.stopAt("map.delete.found", isNode(o))
	var okA bool
	doneA := goStep(t, func() { okA = m.base.Delete(k) })
	stA.waitReached(t, doneA)
	expand.Release()
	waitDone(t, done0, "Set of the 65th key")
	okB := m.base.Delete(k)
	stA.Release()
	waitDone(t, doneA, "Delete(k) of GA")

	if okA || !okB {
		t.Errorf("Delete(k) of GA = %v and of GB = %v, want false and true", okA, okB)
	}
	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found after Delete(k)")
	}
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}

// D2 and the GC: GA finds k in the copy C after the expand ended, claims the
// origin O of C in the old array, and stops before it deletes C. Nothing but
// GA keeps the old array then. A GC must not take it: GB, which finds C too,
// must claim O and lose.
func Test_D2OriginStaysUntilTheCopyIsDeleted(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys)
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	c := (*elist_head.ListHead)(nodeOf(item.(*skiplistmap.SampleItem)))
	if elist_head.FindOrigin(c) == c {
		t.Fatalf("the move of k is not known before the Delete")
	}

	s := newStepper(t)
	stA := s.stopAt("map.delete.claimed", isNode(nodeOf(item.(*skiplistmap.SampleItem))))
	var okA bool
	doneA := goStep(t, func() { okA = m.base.Delete(k) })
	stA.waitReached(t, doneA)
	for i := 0; i < 20 && elist_head.FindOrigin(c) != c; i++ {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	okB := m.base.Delete(k)
	stA.Release()
	waitDone(t, doneA, "Delete(k) of GA")

	if okA == okB {
		t.Errorf("Delete(k) of GA = %v and of GB = %v, want one true", okA, okB)
	}
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}

// A Delete that lost its claim must not touch an item stored again: GB finds
// u, an item of StoreItem, and stops; a Purge of the key of u returns true,
// and GB then loses its claim on u; StoreItem stores u again, and GB goes on.
func Test_D2LostDeleteLeavesAnItemStoredAgain(t *testing.T) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	u := &items[0]
	m := newStepMap()
	setKeys(t, m, keys[1:])
	if !m.base.StoreItem(u) {
		t.Fatalf("StoreItem(u) returned false")
	}

	s := newStepper(t)
	found := s.stopAt("map.delete.found", isNode(nodeOf(u)))
	doneB := goStep(t, func() { m.base.Delete(keys[0]) })
	found.waitReached(t, doneB)
	if !m.base.Purge(keys[0]) {
		t.Fatalf("Purge(u) returned false")
	}
	claimed := s.stopAt("map.delete.claimed", isNode(nodeOf(u)))
	found.Release()
	claimed.waitReached(t, doneB)
	if !m.base.StoreItem(u) {
		t.Fatalf("StoreItem(u) after Purge(u) returned false")
	}
	claimed.Release()
	waitDone(t, doneB, "Delete(u) of GB")

	assertStoredInOrder(t, m, keys)
}

// A Delete that lost its claim on the origin still deletes the copies of the
// item it found: GL finds k in the old array, x, and stops; the pool grows;
// GW finds the copy x', claims the origin x, which is the item of GL, and
// stops before it marks the line; GL loses its claim and returns false.
// Get(k) must not find k then.
func Test_D2LostDeleteDeletesTheCopiesOfItsItem(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	x := nodeOf(item.(*skiplistmap.SampleItem))

	s := newStepper(t)
	found := s.stopAt("map.delete.found", isNode(x))
	doneL := goStep(t, func() { m.base.Delete(k) })
	found.waitReached(t, doneL)
	m.Set(keys[n], &list_head.ListHead{})
	item, ok = m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found after the expand")
	}
	xc := nodeOf(item.(*skiplistmap.SampleItem))
	claimed := s.stopAt("map.delete.claimed", isNode(xc))
	doneW := goStep(t, func() { m.base.Delete(k) })
	claimed.waitReached(t, doneW)
	found.Release()
	waitDone(t, doneL, "Delete(k) of GL")

	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found after Delete(k) of GL returned false")
	}
	claimed.Release()
	waitDone(t, doneW, "Delete(k) of GW")
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}

// D2 and the GC over two expands: GB finds k in the first copy x1 and stops
// before its claim. The pool grows again, and GA deletes k from the second
// copy x2, claiming the origin x in the first array. A GC then takes the
// first array, and GB claims x1, the oldest node of the line still kept: GA
// marked it deleted before it let x go, and GB must return false.
func Test_D2DeleteOfAMiddleCopyAfterTheOriginIsGone(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(2*n + 2)
	m := newStepMap()
	setKeys(t, m, keys[:n+1])
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	x1 := nodeOf(item.(*skiplistmap.SampleItem))

	s := newStepper(t)
	stB := s.stopAt("map.delete.found", isNode(x1))
	var okB bool
	doneB := goStep(t, func() { okB = m.base.Delete(k) })
	stB.waitReached(t, doneB)
	setKeys(t, m, keys[n+1:])
	item, ok = m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found after the second expand")
	}
	x2 := (*elist_head.ListHead)(nodeOf(item.(*skiplistmap.SampleItem)))
	if elist_head.FindOrigin(x2) == x2 {
		t.Fatalf("the moves of k are not known")
	}
	okA := m.base.Delete(k)
	for i := 0; i < 20 && elist_head.FindOrigin(x2) != (*elist_head.ListHead)(x1); i++ {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if elist_head.FindOrigin(x2) != (*elist_head.ListHead)(x1) {
		t.Fatalf("the first array is still kept after the GC")
	}
	stB.Release()
	waitDone(t, doneB, "Delete(k) of GB")

	if okA == okB {
		t.Errorf("Delete(k) of GA = %v and of GB = %v, want one true", okA, okB)
	}
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}

// D2 over two expands: GA finds k in the first array and stops before it
// claims it. The pool grows twice, and the second expand leads the entry of
// the first one to the last array. GB deletes k from the last array; GA must
// return false.
func Test_D2DeletesAcrossTwoExpands(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(2*n + 2)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]
	item, ok := m.base.LoadItem(k)
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	o := nodeOf(item.(*skiplistmap.SampleItem))

	s := newStepper(t)
	stA := s.stopAt("map.delete.found", isNode(o))
	var okA bool
	doneA := goStep(t, func() { okA = m.base.Delete(k) })
	stA.waitReached(t, doneA)
	setKeys(t, m, keys[n:])
	okB := m.base.Delete(k)
	stA.Release()
	waitDone(t, doneA, "Delete(k) of GA")

	if okA || !okB {
		t.Errorf("Delete(k) of GA = %v and of GB = %v, want false and true", okA, okB)
	}
	if _, ok := m.Get(k); ok {
		t.Errorf("Get(k) found after Delete(k)")
	}
	assertStoredInOrder(t, m, withoutKey(keys, 5))
}
