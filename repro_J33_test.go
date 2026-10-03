//go:build stephook

package skiplistmap

import (
	"testing"
	"unsafe"
)

// RangeItem walks the list of entries with Next(WaitNoM()) of elist. When the
// next node is marked, nextWaitNoMark reads it again, and after 100 reads
// that all see a mark it returns nil. RangeItem does not check for nil and
// calls Empty on it.
//
// Map h holds two keys. G1 calls RangeItem and stops in f on the first item,
// a. G2 purges the other key, b, which comes after a in the list, and stops
// at "del.marked" of elist in MarkForDelete of b, when both links of b are
// marked and b is still linked. G1 resumes: f returns true and RangeItem
// walks on from a. When it reaches the node before b, Next(WaitNoM()) reads
// b, marked, 100 times and returns nil, and RangeItem calls Empty on nil.
func Test_ReproJ33_RangeItemGetsNilBesideItemBeingPurged(t *testing.T) {
	h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true))
	m23DirectModes(t)
	k1, k2 := m23KeyWithTop(1), m23KeyWithTop(2)
	n1, n2 := m23StoredNode(t, h, k1), m23StoredNode(t, h, k2)

	reached, release := make(chan struct{}), make(chan struct{})
	var first unsafe.Pointer
	done1, p1 := m23Go(func() {
		h.RangeItemForTest(func(item *TestEntry[StringKey, any]) bool {
			if first == nil {
				first = unsafe.Pointer(item.PtrListHead())
				close(reached)
				<-release
			}
			return true
		})
	})
	select {
	case <-reached:
	case <-done1:
		t.Fatalf("RangeItem of G1 returned before it called f: %v", *p1)
	}
	var other string
	var nodeB unsafe.Pointer
	switch first {
	case n1:
		other, nodeB = k2, n2
	case n2:
		other, nodeB = k1, n1
	default:
		t.Fatalf("f got %p, neither of the two items %p, %p", first, n1, n2)
	}

	stB := m23At("elist.del.marked", m23IsNode(nodeB))
	m23Hooks(t, stB)
	done2, p2 := m23Go(func() { h.Purge(StringKey(other)) })
	stB.waitReached(t, done2)

	close(release)
	m23Wait(t, done1, "RangeItem of G1")
	stB.Release()
	m23Wait(t, done2, "Purge of G2")
	if *p2 != nil {
		t.Fatalf("Purge of G2 panicked: %v", *p2)
	}
	if *p1 != nil {
		t.Errorf("RangeItem of G1 panicked while b was being purged: %v", *p1)
	}
}
