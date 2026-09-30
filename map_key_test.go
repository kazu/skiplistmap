package skiplistmap

import (
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
)

// Adjacent storage must not turn a boundary into an invalidated item cursor.
// Giving each sentinel a matching header makes an accidental header read
// deterministic without reading outside an allocation in the test itself.
func Test_CollisionWalkStopsAtMapBoundaries(t *testing.T) {
	const reverse = uint64(42)
	head, tail := &MapHead{reverse: reverse}, &MapHead{reverse: reverse}
	h := &Map{head: &head.ListHead, tail: &tail.ListHead}
	elist_head.InitAsEmpty(h.head, h.tail)
	e := NewEntryMap("present", 1)
	e.reverse, e.conflict = reverse, 1
	e.ListHead.Init()
	if _, err := h.tail.InsertBefore(e.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	got, retry := h.matchEntry(e, reverse, 2, nil, false, false)
	if got != nil || retry {
		t.Fatalf("absent collision: got %v, retry %v", got, retry)
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(head)
	runtime.KeepAlive(tail)
}

// A structurally comparable type can contain an interface holding a slice.
// Such a key still needs the legacy hash-only fallback rather than a panic.
func Test_NonComparableKeyInsideInterface(t *testing.T) {
	type key struct{ Value interface{} }
	if !equalKeys(key{[]int{1}}, key{[]int{1}}) {
		t.Fatal("non-comparable custom key lost hash-only identity")
	}
	if equalKeys(key{1}, key{2}) {
		t.Fatal("comparable custom keys were not compared")
	}
}
