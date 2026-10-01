package skiplistmap

import (
	"reflect"
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
	h := &Map[StringKey, any]{head: &head.ListHead, tail: &tail.ListHead}
	elist_head.InitAsEmpty(h.head, h.tail)
	e := NewEntryMap[StringKey, any]("present", 1)
	e.reverse, e.conflict = reverse, 1
	e.ListHead.Init()
	if _, err := h.tail.InsertBefore(e.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	got, retry := h.matchEntry(e, reverse, 2, "", false, false)
	if got != nil || retry {
		t.Fatalf("absent collision: got %v, retry %v", got, retry)
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(head)
	runtime.KeepAlive(tail)
}

// A structurally comparable type can contain an interface holding a slice.
// Its Equal method must compare the payload without interface == panics.
func Test_NonComparableKeyInsideInterface(t *testing.T) {
	if !equalKeys[nonComparableTestKey, any](nonComparableTestKey{[]int{1}}, nonComparableTestKey{[]int{1}}) {
		t.Fatal("equal non-comparable keys differed")
	}
	if equalKeys[nonComparableTestKey, any](nonComparableTestKey{1}, nonComparableTestKey{2}) {
		t.Fatal("comparable custom keys were not compared")
	}
}

type nonComparableTestKey struct{ Value any }

func (k nonComparableTestKey) KeyHash() (uint64, uint64) { return 1, 1 }
func (k nonComparableTestKey) Equal(other nonComparableTestKey) bool {
	return reflect.DeepEqual(k.Value, other.Value)
}
