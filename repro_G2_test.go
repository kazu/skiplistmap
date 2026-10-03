//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// storeAdjacent stores items of three keys a < b < c that share the top 4
// bits of the reversed hash, so they are adjacent in the list.
func storeAdjacent(t *testing.T) (*WrapHMap, []string, []skiplistmap.SampleItem[skiplistmap.StringKey, any]) {
	t.Helper()
	m := newStepMap()
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	for i := range items {
		if !m.base.StoreItem(&items[i]) {
			t.Fatalf("StoreItem(%q) failed", keys[i])
		}
	}
	return m, keys, items
}

// Delete(b) marks b deleted (SampleItem.Delete sets mapIsDeleted) and leaves
// b linked between a and c, so StoreItem of the same object returns false and
// leaves the list as it is. Before the fix, StoreItem(b) initialized the list
// head of b while b was still linked: b pointed to itself, a still pointed to
// b, and b and c were no longer found.
func Test_ReproG2StoreItemAgainAfterDelete(t *testing.T) {
	m, keys, items := storeAdjacent(t)
	a, b, c := items[0].PtrListHead(), items[1].PtrListHead(), items[2].PtrListHead()
	if !m.base.Delete(skiplistmap.StringKey(keys[1])) {
		t.Fatalf("Delete(%q) = false", keys[1])
	}
	runWithDeadline(t, 10*time.Second, func() {
		if m.base.StoreItem(&items[1]) {
			t.Errorf("StoreItem(%q) of an item still linked = true", keys[1])
		}
		if p, n := b.DirectPrev(), b.DirectNext(); a.DirectNext() != b || p != a || n != c {
			t.Errorf("b (%p) links prev %p and next %p, want a %p and c %p", b, p, n, a, c)
		}
		assertStoredInOrder(t, m, []string{keys[0], keys[2]})
	})
}

// Purge(b) takes b out of the list, but only a fresh copy can be stored again.
func Test_StoreItemAgainAfterPurge(t *testing.T) {
	m, keys, items := storeAdjacent(t)
	if !m.base.Purge(skiplistmap.StringKey(keys[1])) {
		t.Fatalf("Purge(%q) = false", keys[1])
	}
	if !items[1].PtrListHead().IsSingle() {
		t.Fatalf("Purge(%q) left b linked", keys[1])
	}
	runWithDeadline(t, 10*time.Second, func() {
		if m.base.StoreItem(&items[1]) {
			t.Errorf("retired StoreItem(%q) after Purge = true", keys[1])
		}
		fresh := items[1].Copy()
		defer runtime.KeepAlive(fresh)
		if !m.base.StoreItem(fresh) {
			t.Errorf("StoreItem copy of %q after Purge = false", keys[1])
		}
		assertStoredInOrder(t, m, keys)
	})
}
