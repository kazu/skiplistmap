//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// G3: keys x < k < z; the map m holds x and z, and the item u with key k,
// stored by StoreItem. The item v has the key k too.
//
//  1. GD: m.Delete(k) finds u and stops at map.delete.found, before it
//     claims u.
//  2. m.Purge(k) claims u, takes it out of the list and returns true.
//  3. GS: m.StoreItem(u) takes u for the place between x and z, and stops at
//     elist.add.cas1, before it links u from x.
//  4. m.StoreItem(v) links v between x and z and returns true.
//  5. GS goes on: its CAS of x.next fails on v, it puts u back, and returns.
//  6. GD goes on and returns.
//
// The result must be linearizable. GD deletes k only while m holds it: after
// the store that links k first, and before the other one. So if GD returns
// true, the item of k at the end is u with the value of u, or v with the value
// of v. m.Len() counts the keys that m holds at the end. Before the fix, the
// take of u by GS cleared the delete that the Purge left, and GS stored the
// value of u into v and returned true; GD then claimed u, which no list held,
// returned true and counted k out, while m held v with the value of u.
func Test_G3DeleteStoppedAfterItsLookupLeavesTheItemOfItsKeyStoredMeanwhile(t *testing.T) {
	keys := adjacentKeys(3)
	items := newStepItems([]string{keys[1], keys[1]})
	u, v := &items[0], &items[1]
	uValue, vValue := u.Value(), v.Value()
	m := newStepMap()
	setKeys(t, m, []string{keys[0], keys[2]})
	if !m.base.StoreItem(u) {
		t.Fatalf("StoreItem(u) returned false")
	}

	s := newStepper(t)
	stD := s.stopAt("map.delete.found", isNode(nodeOf(u)))
	var okD bool
	doneD := goStep(t, func() { okD = m.base.Delete(keys[1]) })
	stD.waitReached(t, doneD)
	if !m.base.Purge(keys[1]) {
		t.Fatalf("Purge(k) returned false")
	}
	stS := s.stopAt("elist.add.cas1", isNode(nodeOf(u)))
	var okS bool
	doneS := goStep(t, func() { okS = m.base.StoreItem(u) })
	stS.waitReached(t, doneS)
	if !m.base.StoreItem(v) {
		t.Fatalf("StoreItem(v) returned false")
	}
	stS.Release()
	waitDone(t, doneS, "StoreItem(u)")
	stD.Release()
	waitDone(t, doneD, "Delete(k)")

	found := 0
	for _, k := range keys {
		if _, ok := m.Get(k); ok {
			found++
		}
	}
	if got := m.base.Len(); got != found {
		t.Errorf("Len() = %d, but Get finds %d keys (StoreItem(u) = %v, Delete(k) = %v)", got, found, okS, okD)
	}
	item, ok := m.base.LoadItem(keys[1])
	if okD && ok {
		switch {
		case item == skiplistmap.MapItem(u) && item.Value() == uValue:
		case item == skiplistmap.MapItem(v) && item.Value() == vValue:
		default:
			t.Errorf("after Delete(k) returned true, the item of k is %p with the value %p; want u %p with the value of u %p, or v %p with the value of v %p (StoreItem(u) = %v)",
				item, item.Value(), u, uValue, v, vValue, okS)
		}
	}
}
