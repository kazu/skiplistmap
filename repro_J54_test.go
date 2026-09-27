//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// Keys p < a < n in a map without the embedded pool that never splits a
// bucket; the map holds p and n, and two goroutines store the one item a,
// which is not linked yet, at the same time.
//
//  1. G2: StoreItem(a) misses a and stops at map.set.beforeInit, before
//     _set runs Init on the list node of a.
//  2. G1: StoreItem(a) misses a too, runs Init on a, finds n as the position
//     and reads p as its previous node. Its first CAS moves p.next from n to
//     a, and it stops at elist.add.cas2, before the CAS that moves n.prev
//     from p to a.
//  3. G2 resumes: Init zeroes the links of a in the middle of the link of
//     G1, while p.next is a. add2 of G2 then walks from p to a and finds no
//     position there; if it tries to link a again, it stops at
//     elist.insert.begin or elist.add.cas1.
//  4. G1 resumes: its second CAS moves n.prev from p to a, which Init
//     unlinked, and StoreItem(a) returns true.
//
// The list of entries is broken: p.next and n.prev point to a, but a does
// not point back, so the walk from the head stops at a before the tail and
// never reaches n.
func Test_J54StoreSameUnlinkedItemInitsInsideOtherLink(t *testing.T) {
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	itP, itA, itN := &items[0], &items[1], &items[2]
	p, a, n := itP.PtrListHead(), itA.PtrListHead(), itN.PtrListHead()
	m := newStepMap()
	m.base.StoreItem(itP)
	m.base.StoreItem(itN)

	s := newStepper(t)
	lateInit := s.stopAt("map.set.beforeInit", isNode(nodeOf(itA)))
	var ok1, ok2 bool
	done2 := goStep(t, func() { ok2 = m.base.StoreItem(itA) })
	lateInit.waitReached(t, done2)

	cas2 := s.stopAt("elist.add.cas2", isNode(nodeOf(itA)))
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(itA) })
	cas2.waitReached(t, done1)
	if p.DirectNext() != a || n.DirectPrev() != p {
		t.Fatalf("G1 is not between its two CASes: want p.next == a, n.prev == p")
	}

	relinkBegin := s.stopAt("elist.insert.begin", isNode(nodeOf(itA)))
	relinkCas := s.stopAt("elist.add.cas1", isNode(nodeOf(itA)))
	waitLinked := s.stopAt("map.set.waitLinked", isNode(nodeOf(itA)))
	lateInit.Release()
	switch waitFirst(t, "StoreItem(a) of G2", done2, relinkBegin.reached, relinkCas.reached, waitLinked.reached) {
	case 0:
		t.Logf("StoreItem(a) of G2 returned without linking a again")
	case 3:
		t.Logf("StoreItem(a) of G2 waits for the link of G1 without Init")
	default:
		t.Logf("StoreItem(a) of G2 stopped before linking a again")
	}
	waitLinked.Release()
	t.Logf("after the Init of G2: p.next == a %v, a.prev == p %v, a.next == n %v, a is single %v",
		p.DirectNext() == a, a.DirectPrev() == p, a.DirectNext() == n, a.IsSingle())

	cas2.Release()
	waitDone(t, done1, "StoreItem(a) of G1")
	t.Logf("after G1: p.next == a %v, n.prev == a %v, a.prev == p %v, a.next == n %v",
		p.DirectNext() == a, n.DirectPrev() == a, a.DirectPrev() == p, a.DirectNext() == n)

	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Errorf("%v", err)
	} else {
		assertStoredInOrder(t, m, keys)
	}

	relinkBegin.Release()
	relinkCas.Release()
	waitDone(t, done2, "StoreItem(a) of G2")
	t.Logf("StoreItem(a) of G1 = %v, of G2 = %v", ok1, ok2)
	runtime.KeepAlive(items)
}
