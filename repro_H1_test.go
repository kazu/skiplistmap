//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// sameItemStores holds a map in which two goroutines stored the same item a
// at the same time, and a third goroutine storing b.
type sameItemStores struct {
	m     *WrapHMap
	s     *stepper
	keys  []string
	items []skiplistmap.SampleItem

	p, c, b, a *elist_head.ListHead

	rollback *stepStop // b stopped before the CAS that undoes its first CAS
	aDone    <-chan struct{}
	bDone    <-chan struct{}
}

// startSameItemStores runs the steps shared by the tests of H1 and H2 on keys
// p < c < b < a in a map that holds p, and returns with b linked, or with b
// stopped at elist.add.rollback when its second CAS fails.
//
//  1. G2: StoreItem(a) finds a not linked and stops at map.storeItem.checked,
//     before it takes the mapIsBusy of a, so that G1 can take it.
//  2. G1: StoreItem(a) runs to the end: p -> a -> tail.
//  3. I3: StoreItem(b) finds a as the position, reads p as its previous node,
//     moves p.next from a to b by its first CAS and stops at elist.add.cas2.
//  4. G2 resumes: StoreItem(a) finds a linked by G1 and returns false.
//  5. I3 resumes: its second CAS finds a.prev to be p and links b.
func startSameItemStores(t *testing.T) *sameItemStores {
	t.Helper()
	x := &sameItemStores{keys: adjacentKeys(4), m: newStepMap()}
	x.items = newStepItems(x.keys)
	itP, itC, itB, itA := &x.items[0], &x.items[1], &x.items[2], &x.items[3]
	x.p, x.c, x.b, x.a = itP.PtrListHead(), itC.PtrListHead(), itB.PtrListHead(), itA.PtrListHead()
	m := x.m
	m.base.StoreItem(itP)

	x.s = newStepper(t)
	s := x.s
	lateInit := s.stopAt("map.storeItem.checked", isNode(nodeOf(itA)))
	x.aDone = goStep(t, func() { m.base.StoreItem(itA) })
	lateInit.waitReached(t, x.aDone)

	m.base.StoreItem(itA)
	if x.p.DirectNext() != x.a || x.a.DirectPrev() != x.p {
		t.Fatalf("StoreItem(a) did not link a after p")
	}

	cas2 := s.stopAt("elist.add.cas2", isNode(nodeOf(itB)))
	x.rollback = s.stopAt("elist.add.rollback", isNode(nodeOf(itB)))
	x.bDone = goStep(t, func() { m.base.StoreItem(itB) })
	cas2.waitReached(t, x.bDone)
	if x.p.DirectNext() != x.b || x.a.DirectPrev() != x.p {
		t.Fatalf("StoreItem(b) is not between its two CASes: want p.next == b, a.prev == p")
	}

	relinkBegin := s.stopAt("elist.insert.begin", isNode(nodeOf(itA)))
	relinkCas := s.stopAt("elist.add.cas1", isNode(nodeOf(itA)))
	lateInit.Release()
	switch waitFirst(t, "late StoreItem(a)", x.aDone, relinkBegin.reached, relinkCas.reached) {
	case 0:
		t.Logf("late StoreItem(a) returned without linking a again")
	default:
		t.Logf("late StoreItem(a) stopped before linking a again")
	}
	if !x.a.IsSingle() {
		t.Logf("a stays linked after the late StoreItem(a)")
	}

	cas2.Release()
	if waitFirst(t, "StoreItem(b)", x.bDone, x.rollback.reached) == 0 {
		t.Logf("the second CAS of StoreItem(b) did not fail")
	}
	return x
}

// finish releases the stopped goroutines and waits for them.
func (x *sameItemStores) finish(t *testing.T) {
	t.Helper()
	x.rollback.Release()
	waitDone(t, x.bDone, "StoreItem(b)")
	x.s.mu.Lock()
	for _, st := range x.s.stops {
		st.Release()
	}
	x.s.mu.Unlock()
	waitDone(t, x.aDone, "late StoreItem(a)")
	runtime.KeepAlive(x.items)
}

// logKeys logs the keys p, c, b, a and their reversed hashes.
func (x *sameItemStores) logKeys(t *testing.T) {
	t.Helper()
	for i, name := range []string{"p", "c", "b", "a"} {
		t.Logf("%s: key %q reverse %016x", name, x.keys[i], reverseOf(x.keys[i]))
	}
}

// check reports a break of the entry list and each of the stored keys that
// Get cannot find.
func (x *sameItemStores) check(t *testing.T, stored ...string) {
	t.Helper()
	if err := skiplistmap.StepCheckLists(x.m.base); err != nil {
		t.Errorf("%v", err)
	}
	for _, k := range stored {
		if _, ok := x.m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
}

// H1: a scene other than the three that the report lists for the second CAS
// of an insertion: two goroutines store the same item a at the same time,
// while the insert of b before a is between its two CASes. Keys p < c < b < a,
// the map holds p.
//
// After the steps of startSameItemStores, b is linked: p -> b -> a.
//
//  6. I4: StoreItem(c) finds b as the position after p, reads p as the
//     previous node of b, and links c: p.next from b to c, b.prev from p to c.
//
// The entry list must then be whole and hold p, c, b and a. In the break of
// 3.4.12, it stops at b before its tail, and a and b cannot be found.
func Test_StepStoreSameItemConcurrentlyRollbackFails(t *testing.T) {
	x := startSameItemStores(t)
	x.logKeys(t)
	x.m.base.StoreItem(&x.items[1])
	if x.p.DirectNext() != x.c || x.c.DirectNext() != x.b {
		t.Logf("StoreItem(c) did not link c between p and b")
	}
	x.rollback.Release()
	waitDone(t, x.bDone, "StoreItem(b)")
	if x.c.DirectNext() == x.b && x.b.DirectNext() == x.b {
		t.Errorf("c links to b, but the failed rollback CAS left b unlinked (3.4.12)")
	}
	x.check(t, x.keys...)
	x.finish(t)
}
