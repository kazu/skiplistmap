//go:build stephook

package skiplistmap_test

import "testing"

// H2: 3.4.9 inside the map under the conditions of the report (3.2.1, 3.2.5
// and 2.3 fixed, no StoreItem again after Delete, 3.1.2 not fixed by
// freezing): two goroutines store the same item a at the same time. Keys
// p < c < b < a, the map holds p; c is not stored.
//
// After the steps of startSameItemStores, b is linked: p -> b -> a. b must be
// linked, and the entry list must be whole and hold p, b and a. In the break
// of 3.4.9, p.next leads to a node out of the list, the entry list stops there
// before its tail, and b cannot be found although StoreItem(b) returned true.
func Test_StepStoreSameItemConcurrentlyInitsLinkedNext(t *testing.T) {
	x := startSameItemStores(t)
	x.logKeys(t)
	x.rollback.Release()
	waitDone(t, x.bDone, "StoreItem(b)")
	if x.p.DirectNext() == x.a && x.a.DirectNext() == x.a {
		t.Errorf("the rollback CAS of b moved p.next back to a, which Init unlinked (3.4.9)")
	}
	if x.b.IsSingle() {
		t.Errorf("StoreItem(b) returned, but b is not linked")
	}
	x.check(t, x.keys[0], x.keys[2], x.keys[3])
	x.finish(t)
}
