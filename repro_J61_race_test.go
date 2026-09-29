//go:build stephook && race

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k1 < k3 < k4. G2 gets k1:
// searchKey returns the slot of k1 and _get stops at get.found, before it
// reads the reverse and the conflict of the slot. The test then deletes k1
// with Delete, which only marks the slot. G1 sets k2 and stops at
// set.newKeyLock, before it locks the muPool of the bucket. The test lets G2
// run to its end alone on the only P: _get reads the reverse and the conflict
// of the slot with plain reads, and returns without taking a lock. G1 then
// runs on: getWithFn finds the deleted slot of k1 by bsearchFromFreeList
// (foundFree) and reuses it, clearing its reverse with CompareAndSwapUint64
// and its conflict with StoreUint64. Nothing orders the reads of G2 before the
// writes of G1, so the race detector reports them.
func Test_J61GetPlainReadRacesFoundFree(t *testing.T) {
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, 5)
	for _, i := range []int{0, 1, 3, 4} {
		if !m.Set(keys[i], &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", keys[i])
		}
	}

	s := newStepper(t)
	stop2 := s.stopAt("map.get.found", nil)
	done2 := goStep(t, func() { m.Get(keys[1]) })
	stop2.waitReached(t, done2)

	if !m.base.Delete(keys[1]) {
		t.Fatalf("Delete(k1) failed")
	}
	stop1 := s.stopAt("map.set.newKeyLock", nil)
	done1 := goStep(t, func() { m.Set(keys[2], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	runOn(t, stop1, stop2, true)
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
