//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// skiplistmap4, the item pool mode. Keys share one pool of the Pool. The
// main goroutine sets 64 keys, which fill the 64 items of the pool; k, one
// of them, holds v1. G2 sets the 65th key, finds the pool full and runs
// _expand, which copies the items, v1 of k included, into a new array and
// stops at pool.expand.copied, before RepaireSliceAfterCopy. The main
// goroutine then sets k to v2: the list of entries still goes through the
// old array, so the lookup finds the old item of k, and Set of a key present
// stores v2 into it without any lock and returns true. When G2 resumes, it
// repairs the links to the new array, whose item of k was copied before and
// holds v1. Get(k) then returns v1: the Set of v2, which returned true
// before the Set of the 65th key returned, is lost.
func Test_J50SetBetweenCopyAndRepairIsLost(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys[:n])
	k := keys[5]
	v1, _ := m.Get(k)
	v2 := &list_head.ListHead{}

	s := newStepper(t)
	st := s.stopAt("map.pool.expand.copied", nil)
	done := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	st.waitReached(t, done)
	if !m.Set(k, v2) {
		t.Fatalf("Set(k, v2) returned false")
	}
	if got, _ := m.Get(k); got != v2 {
		t.Fatalf("Get(k) right after Set(k, v2) does not return v2")
	}
	st.Release()
	waitDone(t, done, "Set of the 65th key")

	got, ok := m.Get(k)
	switch {
	case !ok:
		t.Errorf("Get(k) not found after Set(k, v2)")
	case got == v1:
		t.Errorf("Get(k) returns v1: the Set of v2 between the copy and the repair of _expand is lost")
	case got != v2:
		t.Errorf("Get(k) returns neither v1 nor v2")
	}
	assertStoredIfLinked(t, m, keys)
}
