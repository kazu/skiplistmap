//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k2 < k3 < k4 < k5. G1 sets k1:
// insertToPool stores a new array [k0 _ k2 k3 k4 k5] into the pool, where the
// slot at index 1 for k1 is a new slot with reverse 0, and Set stops at
// set.slotTaken, before it stores the reverse of k1 into that slot. G2 then
// gets k0. sort.Search in bsearchBybucket runs over the 6 slots with
// "reverse >= r(k0)", which is false at index 1 although it is true at
// index 0: it reads index 3 (true), then index 1 (false), then index 2 (true),
// and returns 2. The reverse at index 2 is r(k2), so Get(k0) returns false,
// although k0 was stored before the Get started and was never deleted.
func Test_J59InsertToPoolZeroSlotHidesKey(t *testing.T) {
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, 6)
	for _, i := range []int{0, 2, 3, 4, 5} {
		if !m.Set(keys[i], &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", keys[i])
		}
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.set.slotTaken", nil)
	done1 := goStep(t, func() { m.Set(keys[1], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)
	if n := s.total("map.insertToPool.publish"); n != 1 {
		t.Fatalf("Set(k1) reached insertToPool.publish %d times, want 1", n)
	}

	var found bool
	done2 := goStep(t, func() { _, found = m.Get(keys[0]) })
	waitDone(t, done2, "G2")
	stop1.Release()
	waitDone(t, done1, "G1")

	if !found {
		t.Errorf("Get(k0) = false while Set(k1) held a slot of the pool with reverse 0; k0 was stored before and never deleted")
	}
	assertStoredInOrder(t, m, keys)
}
