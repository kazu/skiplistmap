//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// hashedItem is an item whose key hash is given, so that two keys can share
// the reversed hash and differ only in the conflict.
type hashedItem struct {
	skiplistmap.SampleItem
	k, conflict uint64
}

func (s *hashedItem) KeyHash() (uint64, uint64) {
	return s.k, s.conflict
}

// Items y1 and y2 have the same key K, and x has another key with the same
// reversed hash. Entries of one reversed hash lie in the order of their
// links.
//
//  1. G1: StoreItem(y1) finds no entry of K and stops at elist.insert.begin,
//     before it links y1 after the last entry of the reversed hash.
//  2. StoreItem(y2) links y2 there, and StoreItem(x) links x after y2.
//  3. G1 resumes and reads x as the entry before its place. x is not an
//     entry of K, but y2 before x is: y1 must not be linked as a second
//     entry of K.
func Test_SameKeyBehindAnotherKeyOfTheSameHashIsFound(t *testing.T) {
	items := make([]hashedItem, 3)
	for i, c := range []uint64{1, 1, 2} {
		items[i].K = "same-reverse"
		items[i].k, items[i].conflict = 0x0123456789abcdef, c
		items[i].SetValue(&list_head.ListHead{})
	}
	y1, y2, x := &items[0], &items[1], &items[2]
	m := newStepMap()

	s := newStepper(t)
	stop := s.stopAt("elist.insert.begin", isNode(nodeOf(&y1.SampleItem)))
	done := goStep(t, func() { m.base.StoreItem(y1) })
	stop.waitReached(t, done)
	m.base.StoreItem(y2)
	m.base.StoreItem(x)
	stop.Release()
	waitDone(t, done, "StoreItem(y1)")

	if !y1.PtrListHead().IsSingle() && !y2.PtrListHead().IsSingle() {
		t.Errorf("y1 and y2 are both linked as entries of one key")
	}
	if got := m.base.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	runtime.KeepAlive(items)
}
