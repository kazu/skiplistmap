//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
)

// J30 (3.4.9): the Init that _set runs on the list node of an item stored
// again after Delete (the use of 6.4) leaves the previous node pointing to a
// node that Init unlinked, the same shape that step 4 of the pending
// interleaving of 3.4.9 leaves.
//
// Keys a < b < c share the top 4 bits of the reversed hash, so they are
// adjacent in the list. StoreItem(a), StoreItem(b), StoreItem(c), then
// Delete(b): b stays linked between a and c, marked deleted.
//
// Interleaving, in one goroutine G that stores b again with the same object:
//  1. G: _loadItem does not find b (it is deleted), so _set runs. _set finds
//     b as the entry, chooses a as the node to find the position from, and
//     stops at map.set.beforeInit, before b.Init().
//  2. G resumes: b.Init() zeroes the links of b, which is still linked.
//     add2 starts to find the position of b from a and stops at
//     map.find.begin.
//
// At that point a.next must not point to a node whose links Init zeroed.
// Before a fix it does: a.next is b, and b is self-linked, so the forward
// chain from a ends at b and c is cut off.
func Test_ReproJ30StoreItemAgainInitsLinkedNode(t *testing.T) {
	m := newStepMap()
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	for i := range items {
		if !m.base.StoreItem(&items[i]) {
			t.Fatalf("StoreItem(%q) failed", keys[i])
		}
	}
	a, b, c := nodeOf(&items[0]), nodeOf(&items[1]), nodeOf(&items[2])
	if n := m24Next(a); n != b {
		t.Fatalf("the next of a is %p, want b %p", n, b)
	}
	if !m.base.Delete(keys[1]) {
		t.Fatalf("Delete(%q) = false", keys[1])
	}

	s := newStepper(t)
	before := s.stopAt("map.set.beforeInit", isNode(b))
	done := goStep(t, func() { m.base.StoreItem(&items[1]) })
	before.waitReached(t, done)
	if before.b != a {
		before.Release()
		t.Fatalf("_set chose %p as the start, want a %p", before.b, a)
	}
	find := s.stopAt("map.find.begin", isNode(a))
	before.Release()
	find.waitReached(t, done)

	aNext, cPrev := m24Next(a), m24Prev(c)
	t.Logf("after b.Init(): a.next=%p b.prev=%p b.next=%p c.prev=%p (a=%p b=%p c=%p)",
		aNext, m24Prev(b), m24Next(b), cPrev, a, b, c)
	if aNext == b && m24SelfLinked(b) {
		t.Errorf("a.next points to b, which Init unlinked (b is self-linked); the forward chain from a ends at b and c is cut off")
	}
	find.Release()

	finished := false
	select {
	case <-done:
		finished = true
	case <-time.After(10 * time.Second):
	}
	t.Logf("StoreItem(b) finished: %v", finished)
	runtime.KeepAlive(items)
}
