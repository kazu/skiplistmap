//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/elist_head"
)

// J13 (3.2.3): the report says that in skiplistmap4 an entry gets a mark
// only in the scene of 2.3, between the MarkForDelete and the Init that add2
// runs on a bucket dummy linked by the goroutine that won the split
// (map.go add2, the branch for an element that is not single). That branch
// is not only for dummies: StoreItem of an item that another goroutine is
// linking at the same time reaches it too.
//
// Keys p < a < q in a map that does not split buckets; p and q are stored.
//
//  1. G1: StoreItem(a) does not find a, runs Init on a, finds q as the
//     position and stops at "add2.found", before it links a.
//  2. G2: StoreItem(a) does not find a either (it is not linked yet), runs
//     Init on the unlinked a, finds q as well and stops at "add2.found".
//  3. G1 runs to its end: p -> a -> q.
//  4. G2 resumes: a is not single any more, so add2 runs MarkForDelete on a,
//     which is linked in the list of entries, and stops at "elist.del.marked"
//     with both links of a marked.
//
// No bucket is split, so this is not the scene of 2.3, yet the entry a in
// the list carries the marks of a deletion.
func Test_Repro_J13_ConcurrentStoreItemMarksEntry(t *testing.T) {
	m := newStepMap()
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	for _, i := range []int{0, 2} {
		if !m.base.StoreItem(&items[i]) {
			t.Fatalf("StoreItem(%q) failed", keys[i])
		}
	}
	a := &items[1]

	s := newStepper(t)
	found1 := s.stopAt("map.add2.found", isNode(nodeOf(a)))
	done1 := goStep(t, func() { m.base.StoreItem(a) })
	found1.waitReached(t, done1)

	found2 := s.stopAt("map.add2.found", isNode(nodeOf(a)))
	done2 := goStep(t, func() { m.base.StoreItem(a) })
	found2.waitReached(t, done2)

	found1.Release()
	waitDone(t, done1, "G1 (StoreItem(a))")
	if s.total("map.makeBucket.begin") != 0 {
		t.Fatalf("a bucket was split; the schedule is meant to have none")
	}

	marked := s.stopAt("elist.del.marked", isNode(nodeOf(a)))
	found2.Release()
	marked.waitReached(t, done2)
	prev, next := elist_head.LinkMarks(a.PtrListHead())
	if prev || next {
		t.Errorf("the linked entry a has marks (prev %v, next %v) outside the scene of 2.3: G2's add2 ran MarkForDelete on it", prev, next)
	}
	marked.Release()
	waitDone(t, done2, "G2 (StoreItem(a))")
}
