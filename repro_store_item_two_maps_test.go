//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
)

// Test_StoreItemIntoTwoMapsAtOnceFailsOnOne replays this order:
//
//  1. G1: m1.StoreItem(a) marks a as being linked and stops at
//     elist.insert.begin, before it links a.
//  2. G2: m2.StoreItem(a) finds a marked and waits for G1.
//  3. G1 links a into m1.
//  4. G2 must return false, as it does when it runs after G1: a is linked in
//     m1, and one item cannot be linked into two maps.
func Test_StoreItemIntoTwoMapsAtOnceFailsOnOne(t *testing.T) {
	items := newStepItems(adjacentKeys(1))
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()

	s := newStepper(t)
	stop := s.stopAt("elist.insert.begin", isNode(nodeOf(a)))
	done1 := goStep(t, func() { m1.base.StoreItem(a) })
	stop.waitReached(t, done1)
	var ok2 bool
	done2 := goStep(t, func() { ok2 = m2.base.StoreItem(a) })
	waitAtMost(done2, 200*time.Millisecond)
	if s.total("map.set.waitLinked") == 0 {
		t.Fatalf("m2.StoreItem(a) did not wait for the link of a into m1")
	}
	stop.Release()
	waitDone(t, done1, "m1.StoreItem(a)")
	waitDone(t, done2, "m2.StoreItem(a)")

	if ok2 {
		t.Errorf("m2.StoreItem(a) returned true, but a is linked only into m1")
	}
	if got := m2.base.Len(); got != 0 {
		t.Errorf("m2.Len() = %d, want 0", got)
	}
	runtime.KeepAlive(items)
}
