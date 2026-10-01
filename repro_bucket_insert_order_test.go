//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

type lastKeyItem = skiplistmap.Entry[fixedHashKey, any]

// Items a and b have the same key, whose reversed hash is ^0. add2 finds no
// entry after that key and links the item before the last node of the list
// (add2.bucketInsert or add2.tailInsert). Storing a passes the order check
// and stops before InsertBefore reads the previous node; storing b then links
// b there. When a resumes, it reads b as the previous node: a must not be
// linked after b, which would leave two entries of one key.
func Test_InsertAfterLastKeyKeepsOneEntryOfAKey(t *testing.T) {
	items := make([]lastKeyItem, 2)
	for i := range items {
		items[i].InitEntry(fixedHashKey{"last", ^uint64(0), 1}, &list_head.ListHead{})
	}
	m := newHashStepMap()

	s := newStepper(t)
	stop := s.stopAt("elist.insert.begin", isNode(nodeOf(&items[0])))
	done := goStep(t, func() { m.StoreItem(&items[0]) })
	stop.waitReached(t, done)
	if s.total("map.add2.bucketInsert")+s.total("map.add2.tailInsert") == 0 {
		t.Fatalf("StoreItem(a) did not link a before the last node")
	}
	m.StoreItem(&items[1])
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
	if got := m.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
	runtime.KeepAlive(items)
}
