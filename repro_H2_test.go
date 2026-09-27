//go:build stephook

package skiplistmap_test

import "testing"

// H2: 3.4.9 happens inside the map under the conditions of the report (3.2.1,
// 3.2.5 and 2.3 fixed, no StoreItem again after Delete, 3.1.2 not fixed by
// freezing): two goroutines store the same item a at the same time. Keys
// p < c < b < a, the map holds p; c is not stored.
//
// After the steps of startSameItemStores, b is stopped before the rollback
// CAS, with p.next == b, b.next == a, and a unlinked by the Init of the late
// StoreItem(a).
//
//  6. I3 resumes: the rollback CAS moves p.next from b back to a, which is no
//     longer linked. rollback(b) zeroes the links of b. add2 of b retries from
//     p and stops at the self-linked a: no position, and StoreItem(b) returns
//     true with b unlinked.
//
// This is the break of 3.4.9: p.next points to a node that Init unlinked, the
// entry list stops at a before its tail, and b cannot be found although
// StoreItem(b) returned true.
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
