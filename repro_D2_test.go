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
// and claims C. One of the two Delete must return false, and Len must go
// down by one; both returned true and Len went down by two.
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
// must claim O and lose. When a GC took the old array, the move was
// forgotten, GB claimed C itself, and both Delete returned true.
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
