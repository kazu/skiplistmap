//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// testSetCopiesAfterAMove makes the pool move the item O of a key k that a
// Set G1 has linked from the node before it only (skiplistmap4). G1 stops
// between the CASes of its insert, and other runs next to it: it finds O.
// Two more Set fill the pool and expand it; the expand takes O as not linked
// and does not copy it, and the insert of G1 fails. G1 copies O to its copy C
// itself after the move, and stops after it read O. other goes on, and then
// G1, which links C.
func testSetCopiesAfterAMove(t *testing.T, stopOther string, other func(m *WrapHMap, k string)) (*WrapHMap, string) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	// the pool hands out its slots in the order of the Set calls: the item
	// of k, keys[1], is not the last one
	setKeys(t, m, append([]string{keys[0]}, keys[2:n-1]...))

	s := newStepper(t)
	st1 := s.stopAt("elist.add.cas2", nil)
	done1 := goStep(t, func() { m.Set(keys[1], &list_head.ListHead{}) })
	st1.waitReached(t, done1)
	o, _ := s.args("elist.add.cas2")
	st2 := s.stopAt(stopOther, isNode(o))
	done2 := goStep(t, func() { other(m, keys[1]) })
	st2.waitReached(t, done2)
	m.Set(keys[n-1], &list_head.ListHead{})
	st3 := s.stopAt("elist.move.wait", nil)
	done3 := goStep(t, func() { m.Set(keys[n], &list_head.ListHead{}) })
	st3.waitReached(t, done3)
	st3.Release()
	copied := s.stopAt("map.item.copy.read", isSecond(o))
	st1.Release()
	copied.waitReached(t, done1)
	st2.Release()
	waitDone(t, done2, "the other call on k")
	copied.Release()
	waitDone(t, done1, "Set(k)")
	waitDone(t, done3, "the Set that expands the pool")
	return m, keys[1]
}

// other is a Delete of k: it finds O, but G1 holds the mapIsBusy of O, so it
// must return false without writing O or C, and Get finds k.
func Test_SetCopyAfterAMoveRefusesADelete(t *testing.T) {
	var deleted bool
	m, k := testSetCopiesAfterAMove(t, "map.delete.found", func(m *WrapHMap, k string) {
		deleted = m.base.Delete(k)
	})
	if deleted {
		t.Errorf("Delete(k) returned true while Set(k) held its item")
	}
	if _, found := m.Get(k); !found {
		t.Errorf("Get(k) not found after Delete(k) returned false")
	}
}

// other is a Set of k: it finds O and stores its value v into O and C before
// G1 writes C. Get(k) must return v.
func Test_SetCopyAfterAMoveKeepsAnUpdate(t *testing.T) {
	v := &list_head.ListHead{}
	m, k := testSetCopiesAfterAMove(t, "map.update.found", func(m *WrapHMap, k string) {
		m.Set(k, v)
	})
	if got, _ := m.Get(k); got != any(v) {
		t.Errorf("Get(k) = %p, want the value %p of the later Set", got, v)
	}
}
