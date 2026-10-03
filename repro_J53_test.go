//go:build stephook

package skiplistmap_test

import (
	"testing"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Keys a < k < k2 < c in a map with the embedded pool and at most 16 items
// per bucket, so that the four keys stay in one pool; the pool holds a, k
// and c. G2 sets k to v2, and Set stops at set.newKeyLock, before it locks
// the muPool of the bucket and looks the key up. Before the fix the lookup
// came before the lock, and the value went into the item that it had found.
// The main goroutine deletes
// k, which marks the item of k deleted and leaves it in the pool, and then
// sets k2 to v3: the pool finds the deleted slot of k in the place of k2
// (foundFree) and reuses it for k2 under the muPool, which it releases when
// Set(k2) returns. When G2 resumes, the lock is free, and nothing looks at
// the item again: it stores v2 into the item, which now holds k2, and returns
// true. Get(k2) then returns v2, a value that was never set for k2.
func Test_J53SetOfKeyPresentWritesSlotReusedAfterDelete(t *testing.T) {
	keys := adjacentKeys(4)
	a, k, k2, c := keys[0], keys[1], keys[2], keys[3]
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)))
	setKeys(t, m, []string{a, k, c})

	v2, v3 := &list_head.ListHead{}, &list_head.ListHead{}
	itemK, ok := m.base.LoadItemForTest(skiplistmap.StringKey(k))
	if !ok {
		t.Fatalf("LoadItem(k) not found")
	}
	item := unsafe.Pointer(itemK.PtrListHead())
	s := newStepper(t)
	st := s.stopAt("map.set.newKeyLock", nil)
	var ok2 bool
	done := goStep(t, func() { ok2 = m.Set(k, v2) })
	st.waitReached(t, done)
	if !m.Delete(k) {
		t.Fatalf("Delete(k) = false")
	}
	if !m.Set(k2, v3) {
		t.Fatalf("Set(k2, v3) = false")
	}
	it2, ok := m.base.LoadItemForTest(skiplistmap.StringKey(k2))
	if !ok {
		t.Fatalf("LoadItem(k2) not found after Set(k2, v3)")
	}
	t.Logf("Set(k2) reused the slot of k that Set(k, v2) found: %v", unsafe.Pointer(it2.PtrListHead()) == item)
	st.Release()
	waitDone(t, done, "Set(k, v2)")
	t.Logf("Set(k, v2) = %v", ok2)

	switch got, ok := m.Get(k2); {
	case !ok:
		t.Errorf("Get(k2) not found")
	case got == v2:
		t.Errorf("Get(k2) returns v2, the value of Set(k, v2), which went into the slot reused for k2")
	case got != v3:
		t.Errorf("Get(k2) returns neither v3 nor v2")
	}
	for _, x := range []string{a, c} {
		if _, ok := m.Get(x); !ok {
			t.Errorf("Get(%q) not found", x)
		}
	}
}
