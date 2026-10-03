//go:build stephook && race

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k1 < k3 < k4 < k5. G2 gets k4 and
// stops at bsearch.searched: sort.Search loaded the array of the pool with
// atomic.LoadPointer and found index 3 in it. G1 then sets k2 alone on the
// only P: insertToPool makes a new array, copies the items of the pool into
// it with copy (plain writes), and stores the new array into the pool with
// CompareAndSwapPointer in CopyFrom. G2 resumes: itemSlice.reverseAt reads
// the array of the pool with a plain read, gets the new array, and reads the
// reverse at index 3 of the new array with atomic.LoadUint64. The load of G2
// that found the old array does not order anything G1 did to the new one, so
// nothing orders the copy of G1 before the load of G2, and the race detector
// reports the two.
func Test_J36ReverseAtRacesInsertToPoolCopy(t *testing.T) {
	m, keys := n2Map(t, 6, 0, 1, 3, 4, 5)

	s := newStepper(t)
	stop2 := s.stopAt("map.bsearch.searched", nil)
	done2 := goStep(t, func() { m.Get(keys[4]) })
	stop2.waitReached(t, done2)

	done1 := runAloneThenRelease(t, stop2, func() { m.Set(keys[2], &list_head.ListHead{}) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
