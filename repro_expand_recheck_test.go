//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// expandAnotherPool makes an item pool of another map expand, which any
// write that started before counts as an expand that overlapped it.
func expandAnotherPool(t *testing.T) {
	t.Helper()
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m := newStepMap()
	setKeys(t, m, keys)
}

// assertLenMatchesKey checks that the length of m counts key once when key
// is stored, and not at all when it is not.
func assertLenMatchesKey(t *testing.T, m *WrapHMap, key string) {
	t.Helper()
	want := 0
	if _, found := m.base.Get(key); found {
		want = 1
	}
	if got := m.base.Len(); got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}
}

// Test_StoreItemRecheckAfterExpandKeepsAReplacedItem replays this order:
//
//  1. G1: StoreItem(a) of key k stops at map.set.beforeInit.
//  2. The item pool of another map expands.
//  3. G1 links a and stops at map.set.expandOverlapped.
//  4. Delete(k) removes a, which stays linked, and StoreItem(b) links b of
//     the same key.
//  5. G1 looks k up again and finds b. a was deleted, not lost to the
//     expand, so StoreItem must not run Init on a while its neighbors
//     still point to it.
func Test_StoreItemRecheckAfterExpandKeepsAReplacedItem(t *testing.T) {
	holdPoolArrays(t)
	m := newStepMap()
	keys := []string{"review-1-store", "review-1-store"}
	items := newStepItems(keys)
	a, b := &items[0], &items[1]

	s := newStepper(t)
	beforeInit := s.stopAt("map.set.beforeInit", isNode(nodeOf(a)))
	overlapped := s.stopAt("map.set.expandOverlapped", isNode(nodeOf(a)))
	done := goStep(t, func() { m.base.StoreItem(a) })
	beforeInit.waitReached(t, done)

	expandAnotherPool(t)
	beforeInit.Release()
	overlapped.waitReached(t, done)
	if !m.base.Delete(keys[0]) {
		t.Fatalf("Delete(k) did not find a")
	}
	if !m.base.StoreItem(b) {
		t.Fatalf("StoreItem(b) returned false")
	}
	overlapped.Release()
	waitDone(t, done, "StoreItem(a)")

	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Fatalf("list broken after StoreItem(a): %v", err)
	}
	assertLenMatchesKey(t, m, keys[0])
}

// Test_StoreItemRecheckAfterExpandCountsAnItemNotLinked replays this order:
//
//  1. G1: StoreItem(a) of key k stops at map.set.beforeInit.
//  2. StoreItem(b) of the same key links b, and the item pool of another
//     map expands.
//  3. G1 finds b in add2 and stores the value of a into b without linking
//     a or counting it in the length.
//  4. The expand overlapped StoreItem(a), whose look-up again finds b. a
//     was not linked, so StoreItem must not lower the length.
func Test_StoreItemRecheckAfterExpandCountsAnItemNotLinked(t *testing.T) {
	holdPoolArrays(t)
	m := newStepMap()
	keys := []string{"review-1-same", "review-1-same"}
	items := newStepItems(keys)
	a, b := &items[0], &items[1]

	s := newStepper(t)
	beforeInit := s.stopAt("map.set.beforeInit", isNode(nodeOf(a)))
	done := goStep(t, func() { m.base.StoreItem(a) })
	beforeInit.waitReached(t, done)
	if !m.base.StoreItem(b) {
		t.Fatalf("StoreItem(b) returned false")
	}
	expandAnotherPool(t)
	beforeInit.Release()
	waitDone(t, done, "StoreItem(a)")

	assertLenMatchesKey(t, m, keys[0])
}
