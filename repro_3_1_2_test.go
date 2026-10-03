//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// holdPoolArrays keeps the collector off for the test, so that the old array
// of a pool that _expand drops stays in place while the list still reaches
// it. Freeing it is 3.1-1; these tests look only at the links. It also puts
// back the traversal mode of lista, which a failed _expand leaves at
// WaitNoMark for the whole process.
func holdPoolArrays(t *testing.T) {
	t.Helper()
	old := debug.SetGCPercent(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(old)
		list_head.DefaultModeTraverse.Option(list_head.Direct())
	})
}

// setKeys sets keys in order and fails the test if one of them is not set.
func setKeys(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	for _, k := range keys {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
}

// withoutKey returns keys without keys[i].
func withoutKey(keys []string, i int) []string {
	return append(append([]string{}, keys[:i]...), keys[i+1:]...)
}

// assertStoredIfLinked runs assertStoredInOrder only when the list of entries
// reaches its tail, because Get loops forever on a list that stops at a
// self-linked node.
func assertStoredIfLinked(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](m.base); err != nil {
		t.Errorf("%v", err)
		return
	}
	assertStoredInOrder(t, m, keys)
}

// Keys a < z < b share one pool. The main goroutine sets 64 keys other than
// z, which fill the 64 items of the pool; a and b take indexes 9 and 10, so
// a.next is b inside the array. G3 sets the 65th key, finds the pool full and
// runs _expand, which copies the items into a new array and stops before
// RepaireSliceAfterCopy. The main goroutine then stores the item z, which
// does not come from the pool. Before the fix, StoreItem linked z between the
// old a and the old b. When G3 resumed, RepaireSliceAfterCopy saw that the
// old a pointed out of the array to z, moved z.prev to the new a, and then
// found that the new a, copied before z was linked, still pointed to b, not
// to z. It returned an error, _expand returned EPoolExpandFail, and the Get of
// G3 panicked with "already deleted". _expand now marks the old a and b
// before it copies them, so StoreItem does not link z between them: its insert
// fails on the marks until the expand ends, and links z between the copies.
func Test_Repro_3_1_2_InsertBetweenCopyAndRepairPanics(t *testing.T) {
	holdPoolArrays(t)
	keys := adjacentKeys(skiplistmap.CntOfPersamepleItemPool + 2)
	kz := keys[10]
	t.Logf("a, z, b = %q, %q, %q", keys[9], kz, keys[11])
	fs := withoutKey(keys, 10)
	z := newStepItems([]string{kz})
	m := newStepMap()
	setKeys(t, m, fs[:skiplistmap.CntOfPersamepleItemPool])

	s := newStepper(t)
	st := s.stopAt("map.pool.expand.copied", nil)
	done := goStep(t, func() { m.Set(fs[skiplistmap.CntOfPersamepleItemPool], &list_head.ListHead{}) })
	st.waitReached(t, done)
	// the insert of z fails on the marks of the old a and b, and StoreItem
	// finds its position again until the expand ends
	var ok bool
	doneZ := goStep(t, func() { ok = m.base.StoreItem(&z[0]) })
	if waitAtMost(doneZ, time.Second) {
		t.Errorf("StoreItem(%q) returned while the pool was being expanded", kz)
	}
	st.Release()
	waitDone(t, done, "Set of the 65th key")
	waitDone(t, doneZ, "StoreItem(z)")
	if !ok {
		t.Fatalf("StoreItem(%q) failed", kz)
	}

	assertStoredIfLinked(t, m, keys)
	runtime.KeepAlive(z)
}

// Keys a < z < b share one pool, and the main goroutine sets 64 keys other
// than z, which fill the pool; a.next is b inside the array. G2 stores the
// item z, which does not come from the pool, and stops at add2.found with the
// old b as its position. The main goroutine sets the 65th key, which runs
// _expand. Before the fix, _expand ran to the end: a and b are linked to each
// other inside the array, so RepaireSliceAfterCopy leaves them, and the list
// went from the new a to the new b. When G2 resumed, it read the old a as the
// previous node of the old b and its two CASes succeeded on the old array:
// StoreItem returned true and the length counted z, but no node of the list
// pointed to z. _expand now marks the old a and b before it copies them: the
// insert of G2 fails on the mark of the old b, and G2 finds its position
// again among the copies.
func Test_Repro_3_1_2_InsertAfterExpandIsLost(t *testing.T) {
	holdPoolArrays(t)
	keys := adjacentKeys(skiplistmap.CntOfPersamepleItemPool + 2)
	kz := keys[10]
	t.Logf("a, z, b = %q, %q, %q", keys[9], kz, keys[11])
	fs := withoutKey(keys, 10)
	z := newStepItems([]string{kz})
	m := newStepMap()
	setKeys(t, m, fs[:skiplistmap.CntOfPersamepleItemPool])

	s := newStepper(t)
	st := s.stopAt("map.add2.found", isNode(nodeOf(&z[0])))
	done := goStep(t, func() { m.base.StoreItem(&z[0]) })
	st.waitReached(t, done)
	if r := skiplistmap.StepEntryReverse(st.b); r != reverseOf(keys[11]) {
		t.Fatalf("StoreItem(%q) stopped before %016x, want b %016x", kz, r, reverseOf(keys[11]))
	}
	// the expand marks the old a and b and runs to the end
	doneSet := goStep(t, func() { setKeys(t, m, fs[skiplistmap.CntOfPersamepleItemPool:]) })
	waitDone(t, doneSet, "Set of the 65th key")
	st.Release()
	waitDone(t, done, "StoreItem(z)")

	assertStoredIfLinked(t, m, keys)
	runtime.KeepAlive(z)
}
