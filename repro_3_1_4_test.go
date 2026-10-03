//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// assertKeyLinkedOnce checks that the map holds keys once each, and that
// Delete(keys[1]) makes keys[1] not found.
func assertKeyLinkedOnce(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	assertStoredInOrder(t, m, keys)
	if !m.base.Delete(skiplistmap.StringKey(keys[1])) {
		t.Fatalf("Delete(%q) = false", keys[1])
	}
	if _, ok := m.Get(keys[1]); ok {
		t.Errorf("Get(%q) found after Delete", keys[1])
	}
}

// Keys a < b < c. The list holds a and c. G2 stores b2, an item of key b: its
// lookup misses b, and it stops at add2.found with c as its position. The
// main goroutine then stores b1, another item of key b: its lookup misses b
// too, since b2 is not linked yet, and it links b1 between a and c. When G2
// resumes, nothing looks up b again. The order check accepts b1 as the left
// neighbour of b2, since their reversed hashes are equal, and b2 is linked
// after b1. The map then holds b twice: Len is 4, RangeItem yields b twice,
// and after Delete(b) marks one of them, Get(b) still finds the other.
func Test_Repro_3_1_4_StoreItemSameNewKeyLinkedTwice(t *testing.T) {
	keys := adjacentKeys(3)
	items := newStepItems([]string{keys[0], keys[1], keys[1], keys[2]}) // a, b1, b2, c
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("map.add2.found", isNode(nodeOf(&items[2])))
	done := goStep(t, func() { m.base.StoreItem(&items[2]) })
	stop.waitReached(t, done)
	m.base.StoreItem(&items[1])
	stop.Release()
	waitDone(t, done, "StoreItem(b2)")

	assertKeyLinkedOnce(t, m, keys)
	runtime.KeepAlive(items)
}

// The same interleaving through Set. Keys a < b < c; the map holds a and c.
// G2 runs Set(b, v2): its lookup misses b, it takes an item that is not the
// last one of its pool, so it holds no lock of the pool, and it stops at
// add2.found. The main goroutine runs Set(b, v1) to the end: its lookup
// misses b too and links a second item of key b. When G2 resumes, it links
// its item after that one. The map then holds b twice.
func Test_Repro_3_1_4_SetSameNewKeyLinkedTwice(t *testing.T) {
	keys := adjacentKeys(3)
	m := newStepMap()
	m.Set(keys[0], &list_head.ListHead{})
	m.Set(keys[2], &list_head.ListHead{})

	rb := reverseOf(keys[1])
	s := newStepper(t)
	stop := s.stopAt("map.add2.found", func(a, b, c unsafe.Pointer) bool {
		return skiplistmap.StepEntryReverse(a) == rb
	})
	done := goStep(t, func() { m.Set(keys[1], &list_head.ListHead{}) })
	stop.waitReached(t, done)
	m.Set(keys[1], &list_head.ListHead{})
	stop.Release()
	waitDone(t, done, "Set(b, v2)")

	assertKeyLinkedOnce(t, m, keys)
}
