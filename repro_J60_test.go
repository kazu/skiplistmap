//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k1 < k3 < k4, and k1 is deleted
// with Delete, which only marks its slot. G1 sets k2: getWithFn finds the
// deleted slot of k1 by bsearchFromFreeList (foundFree), clears its reverse
// and its state to 0, and Set stops at set.slotTaken, before it stores the
// reverse of k2 into that slot. The pool is [k0 _ k3 k4] and the slot at
// index 1 is not deleted and has reverse 0. G2 then gets k0. sort.Search in
// bsearchBybucket runs over the 4 slots with "reverse >= r(k0)", which is
// false at index 1 although it is true at index 0: it reads index 2 (true),
// then index 1 (false), and returns 2. The reverse at index 2 is r(k3), so
// Get(k0) returns false, although k0 was stored before the Get started and was
// never deleted.
func Test_J60FoundFreeZeroSlotHidesKey(t *testing.T) {
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, 5)
	for _, i := range []int{0, 1, 3, 4} {
		if !m.Set(keys[i], &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", keys[i])
		}
	}
	if !m.base.Delete(keys[1]) {
		t.Fatalf("Delete(k1) failed")
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.set.slotTaken", nil)
	done1 := goStep(t, func() { m.Set(keys[2], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)
	if n := s.total("map.insertToPool.publish"); n != 0 {
		t.Fatalf("Set(k2) reached insertToPool.publish %d times, want 0 (the slot of k1 must be reused)", n)
	}
	if n := s.total("map.appendLast.claimed"); n != 0 {
		t.Fatalf("Set(k2) reached appendLast.claimed %d times, want 0 (the slot of k1 must be reused)", n)
	}

	var found bool
	done2 := goStep(t, func() { _, found = m.Get(keys[0]) })
	waitDone(t, done2, "G2")
	stop1.Release()
	waitDone(t, done1, "G1")

	if !found {
		t.Errorf("Get(k0) = false while Set(k2) reused the deleted slot of k1 with reverse 0; k0 was stored before and never deleted")
	}
	assertStoredInOrder(t, m, []string{keys[0], keys[2], keys[3], keys[4]})
}
