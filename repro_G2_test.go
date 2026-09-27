//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"
)

// Storing an item again with the same object after Delete must store it: the
// map must find it, and the list must stay a sound chain.
//
// Sequence, in one goroutine: keys a < b < c share the top 4 bits of the
// reversed hash, so they are adjacent in the list. StoreItem(a), StoreItem(b),
// StoreItem(c). Delete(b) marks b deleted (SampleItem.Delete sets
// mapIsDeleted) and leaves b linked between a and c. StoreItem(b) with the same
// object: _loadItem skips b because it is ignored and reports not found, so
// _set runs. _set finds b as the entry and a as the start, and initializes the
// list head of b while b is still linked: b now points to itself, and a still
// points to b. add2 walks from a, stops at b because b.Next() is b, and gets no
// position; it falls back to a, the order check rejects inserting b after a,
// and b is not linked again. The chain forward from a ends at the self-linked
// b: b and c are no longer found, and nothing clears mapIsDeleted of b.
func Test_ReproG2StoreItemAgainAfterDelete(t *testing.T) {
	m := newStepMap()
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	for i := range items {
		if !m.base.StoreItem(&items[i]) {
			t.Fatalf("StoreItem(%q) failed", keys[i])
		}
	}
	if p := items[1].PtrListHead().DirectPrev(); p != items[0].PtrListHead() {
		t.Fatalf("the prev of b is %p, want a %p", p, items[0].PtrListHead())
	}
	if n := items[1].PtrListHead().DirectNext(); n != items[2].PtrListHead() {
		t.Fatalf("the next of b is %p, want c %p", n, items[2].PtrListHead())
	}

	if !m.base.Delete(keys[1]) {
		t.Fatalf("Delete(%q) = false", keys[1])
	}
	if _, ok := m.Get(keys[1]); ok {
		t.Fatalf("Get(%q) found a deleted key", keys[1])
	}

	runWithDeadline(t, 10*time.Second, func() {
		m.base.StoreItem(&items[1])
		a, b, c := items[0].PtrListHead(), items[1].PtrListHead(), items[2].PtrListHead()
		if n := a.DirectNext(); n != b {
			t.Errorf("the next of a is %p, want b %p", n, b)
		}
		if p, n := b.DirectPrev(), b.DirectNext(); p != a || n != c {
			t.Errorf("b (%p) links prev %p and next %p, want a %p and c %p", b, p, n, a, c)
		}
		assertStoredInOrder(t, m, keys)
	})
}
