//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// lastKeyItem is an item whose reversed hash is the largest, ^0: no entry of
// the list comes after it, and add2 finds no position for it.
type lastKeyItem struct {
	skiplistmap.SampleItem
}

func (s *lastKeyItem) KeyHash() (uint64, uint64) {
	return ^uint64(0), 1
}

// Items a and b have the same key, whose reversed hash is ^0. add2 finds no
// entry after that key and links the item before the last node of the list
// (add2.bucketInsert or add2.tailInsert). Storing a passes the order check
// and stops before InsertBefore reads the previous node; storing b then links
// b there. When a resumes, it reads b as the previous node: a must not be
// linked after b, which would leave two entries of one key.
func Test_InsertAfterLastKeyKeepsOneEntryOfAKey(t *testing.T) {
	items := make([]lastKeyItem, 2)
	for i := range items {
		items[i].K = "last"
		items[i].SetValue(&list_head.ListHead{})
	}
	m := newStepMap()

	s := newStepper(t)
	stop := s.stopAt("elist.insert.begin", isNode(nodeOf(&items[0].SampleItem)))
	done := goStep(t, func() { m.base.StoreItem(&items[0]) })
	stop.waitReached(t, done)
	if s.total("map.add2.bucketInsert")+s.total("map.add2.tailInsert") == 0 {
		t.Fatalf("StoreItem(a) did not link a before the last node")
	}
	m.base.StoreItem(&items[1])
	stop.Release()
	waitDone(t, done, "StoreItem(a)")

	linked := 0
	for i := range items {
		if !items[i].PtrListHead().IsSingle() {
			linked++
		}
	}
	if linked != 1 {
		t.Errorf("%d entries of one key are linked, want 1", linked)
	}
	if got := m.base.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
	runtime.KeepAlive(items)
}
