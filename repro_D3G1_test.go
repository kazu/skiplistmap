//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// D3-G1, what its fix leaves: two StoreItem calls of one item u run at once,
// and a Delete finds u between the two CASes of the insert of one of them.
// Keys x < k < z, where k is the key of u; the map m holds x and z.
//
//  1. S1: m.StoreItem(u) stops at map.find.begin, before add2 looks for the
//     position from x, holding the mapIsBusy of u.
//  2. S2: m.StoreItem(u) must return false: S1 holds u.
//  3. m.Purge(x) returns true, so that S1 finds no position from x.
//  4. S1 inserts u after the dummy of the bucket: it takes u, links u from
//     the dummy by its first CAS and stops at elist.add.cas2.
//  5. GD: m.Delete(k) finds u and must return false: S1 holds u.
//  6. GZ: m.Purge(z) stops at elist.del.marked, after it marked z, so that the
//     second CAS of S1 fails.
//  7. S1 puts u back and stops at map.find.begin, where it looks for the
//     position from x again.
//  8. GZ ends. S1 goes on, links u and returns true.
//
// The result must be linearizable. m holds no key but k at the end. Each
// StoreItem(u) that returns true stores k while m does not hold it, as u is
// the only item of k, and each Delete that returns true removes k; so the
// number of StoreItem calls that return true, less the Deletes that return
// true, is 1 when k is found at the end and 0 otherwise, and m.Len() is that
// number. Before the flag, S2 stopped at elist.insert.begin before it took u,
// GD returned true, and S2 went on after step 8: its insert before z failed,
// and it took u again and linked it, clearing the delete of GD. S1 found u
// linked and returned false, S2 returned true, and u stayed linked and found
// while Len did not count it.
func Test_D3G1DeleteBetweenTheCASesAndTheOtherStoreItemAreRefused(t *testing.T) {
	keys := adjacentKeys(3)
	items := newStepItems(keys[1:2])
	u := &items[0]
	m := newStepMap()
	setKeys(t, m, []string{keys[0], keys[2]})
	xItem, ok := m.base.LoadItem(keys[0])
	if !ok {
		t.Fatalf("LoadItem(x) not found")
	}
	x := nodeOf(xItem.(*skiplistmap.SampleItem))
	zItem, ok := m.base.LoadItem(keys[2])
	if !ok {
		t.Fatalf("LoadItem(z) not found")
	}
	z := nodeOf(zItem.(*skiplistmap.SampleItem))

	s := newStepper(t)
	find1 := s.stopAt("map.find.begin", isNode(x))
	var ok1, ok2, okD bool
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(u) })
	find1.waitReached(t, done1)
	if ok2 = m.base.StoreItem(u); ok2 {
		t.Errorf("the second StoreItem(u) returned true while the first held u")
	}
	if !m.base.Purge(keys[0]) {
		t.Fatalf("Purge(x) returned false")
	}
	cas2 := s.stopAt("elist.add.cas2", isNode(nodeOf(u)))
	find1.Release()
	cas2.waitReached(t, done1)
	doneD := goStep(t, func() { okD = m.base.Delete(keys[1]) })
	waitDone(t, doneD, "Delete(k)")
	if okD {
		t.Errorf("Delete(k) returned true while StoreItem(u) held u")
	}
	stZ := s.stopAt("elist.del.marked", isNode(z))
	doneZ := goStep(t, func() { m.base.Purge(keys[2]) })
	stZ.waitReached(t, doneZ)
	again := s.stopAt("map.find.begin", isNode(x))
	cas2.Release()
	again.waitReached(t, done1)
	stZ.Release()
	waitDone(t, doneZ, "Purge(z)")
	again.Release()
	waitDone(t, done1, "the first StoreItem(u)")

	found := 0
	if _, ok := m.Get(keys[1]); ok {
		found = 1
	}
	stored, deleted := 0, 0
	for _, ok := range []bool{ok1, ok2} {
		if ok {
			stored++
		}
	}
	if okD {
		deleted = 1
	}
	if stored-deleted != found {
		t.Errorf("StoreItem(u) returned %v and %v and Delete(k) returned %v, so Get(k) must find %d keys at the end, but it finds %d",
			ok1, ok2, okD, stored-deleted, found)
	}
	if got := m.base.Len(); got != found {
		t.Errorf("Len() = %d, but Get finds %d keys", got, found)
	}
}
