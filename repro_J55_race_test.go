//go:build stephook && race

package skiplistmap_test

import "testing"

// skiplistmap4, the item pool mode: a map without the embedded pool that
// never splits a bucket holds a < b < c, set with Set, so their items are
// SampleItems of the pool. G1 deletes b: LoadItem finds the item of b and
// Delete stops at delete.found, before it runs Delete of the item. G2 then
// gets b alone on the only P: Get takes no lock, finds the item of b and
// reads its state with the plain read in MapHead.IsIgnored, and returns. G1
// then runs SampleItem.Delete, which sets the deleted bit of the state with a
// plain write. Nothing orders the read of G2 before the write of G1, so the
// race detector reports them.
func Test_J55SampleItemDeleteRacesIsIgnoredOfGet(t *testing.T) {
	keys := adjacentKeys(3)
	m := newStepMap()
	setKeys(t, m, keys)
	if _, ok := m.Get(keys[1]); !ok {
		t.Fatalf("Get(b) = false before the test")
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.delete.found", nil)
	done1 := goStep(t, func() { m.Delete(keys[1]) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Get(keys[1]) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
