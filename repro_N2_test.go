//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/lista_encabezado"
)

// A map with the embedded pool that does not split its buckets holds
// k0 < k1 < k3 < k4 < k5 of the top bucket 3 in one pool. G2 gets k4 and stops
// at bsearch.searched: sort.Search found its index 3 in the array of the pool.
// G1 then sets k2 to its end: insertToPool stores a new array
// [k0 k1 k2 k3 k4 k5] into the pool. When G2 resumes, bsearchBybucket reads
// the reverse at index 3 from the array that the pool holds now, finds k3
// there, and returns no item. Get(k4) returns false although k4 was stored
// before the Get started and was never deleted.
func Test_N2GetMissesKeyMovedByInsert(t *testing.T) {
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, 6)
	for _, i := range []int{0, 1, 3, 4, 5} {
		if !m.Set(keys[i], &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", keys[i])
		}
	}

	s := newStepper(t)
	stop2 := s.stopAt("map.bsearch.searched", nil)
	var found bool
	done2 := goStep(t, func() { _, found = m.Get(keys[4]) })
	stop2.waitReached(t, done2)

	done1 := goStep(t, func() { m.Set(keys[2], &list_head.ListHead{}) })
	waitDone(t, done1, "G1")
	if n := s.total("map.insertToPool.publish"); n != 1 {
		t.Fatalf("Set(k2) reached insertToPool.publish %d times, want 1", n)
	}

	stop2.Release()
	waitDone(t, done2, "G2")
	if !found {
		t.Errorf("Get(k4) = false while Set(k2) moved k4 in the pool; k4 was stored before and never deleted")
	}
	assertStoredInOrder(t, m, keys)
}
