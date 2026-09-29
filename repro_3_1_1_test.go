//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"unsafe"
	"weak"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// topKeys returns n keys whose reversed hashes have top as their top 4 bits.
// Keys with the same top 4 bits modulo 8 take their items from the same
// pool of a map without the embedded pool.
func topKeys(top uint64, n int) []string {
	var keys []string
	for i := 0; len(keys) < n; i++ {
		if k := crashKey(i); reverseOf(k)>>60 == top {
			keys = append(keys, k)
		}
	}
	return keys
}

// setPastExpand runs the interleaving of 3.1-1 on a new map and returns the
// map, the key k1 and the node of the item that Set(k1) linked.
//
// G1 sets k1, the first key of its pool, so it takes index 0 of the 64 items
// of the pool. Index 0 is not the last one, so G1 holds no lock of the pool.
// G1 stops at add2.found, before it links its item. The main goroutine then
// sets 64 keys of the same pool in another region: the first 63 take indexes
// 1 to 63, and the 64th finds the pool full and runs _expand. _expand copies
// the items into a new array while G1's item is still unlinked, so the copy
// is unlinked too, and the new pool replaces the old one in the pool list.
// Then G1 resumes. Before the fix it linked its item, which lies in the old
// array; it links the copy of its item in the new array now.
func setPastExpand(t *testing.T) (m *WrapHMap, k1 string, node unsafe.Pointer) {
	t.Helper()
	m = newStepMap()
	k1 = topKeys(1, 1)[0]
	others := topKeys(9, skiplistmap.CntOfPersamepleItemPool)
	r1 := reverseOf(k1)

	s := newStepper(t)
	st := s.stopAt("map.add2.found", func(a, b, c unsafe.Pointer) bool {
		return skiplistmap.StepEntryReverse(a) == r1
	})
	done1 := goStep(t, func() { m.Set(k1, &list_head.ListHead{}) })
	st.waitReached(t, done1)
	doneO := goStep(t, func() {
		for _, k := range others {
			if !m.Set(k, &list_head.ListHead{}) {
				t.Errorf("Set(%q) failed", k)
			}
		}
	})
	waitDone(t, doneO, "the Sets of the other keys")
	st.Release()
	waitDone(t, done1, "Set(k1)")

	// Drop every pointer the stepper keeps into the old array.
	elist_head.SetStepHook(nil)
	skiplistmap.SetStepHook(nil)
	s.mu.Lock()
	node = st.a
	st.a, st.b = nil, nil
	s.hits = map[stepHit]int{}
	s.last = map[string][2]unsafe.Pointer{}
	s.mu.Unlock()
	return m, k1, node
}

// After the interleaving of setPastExpand, the item of k1 is linked into the
// list of entries, but it lies in the old array, which no pool holds any
// more. The copy of it in the new array is counted in the length of the new
// pool and linked nowhere.
func Test_Repro_3_1_1_LinkedItemOutsidePools(t *testing.T) {
	m, k1, _ := setPastExpand(t)
	if _, ok := m.Get(k1); !ok {
		t.Fatalf("Get(%q) not found", k1)
	}
	if err := skiplistmap.StepCheckPooledItems(m.base); err != nil {
		t.Errorf("%v", err)
	}
}

// weakItem returns a weak pointer to the item whose node is p and its
// address, without keeping the item alive in the caller's frame.
//
//go:noinline
func weakItem(p unsafe.Pointer) (weak.Pointer[skiplistmap.SampleItem], uintptr) {
	item := skiplistmap.SampleItemFromListHead((*elist_head.ListHead)(p))
	return weak.Make(item), uintptr(unsafe.Pointer(item))
}

//go:noinline
func weakAlive(wp weak.Pointer[skiplistmap.SampleItem]) bool {
	return wp.Value() != nil
}

var reuseSink [][]skiplistmap.SampleItem

// Before the fix, after the interleaving of setPastExpand the old array was
// referenced only by the offsets of the list of entries, which the GC does
// not follow. After two GCs the weak pointer to the item of k1 was nil, so
// the old array was freed, while the list of entries still reached the item.
// Allocating arrays of the same size then reused the freed memory and zeroed
// the item: the list of entries stopped at a self-linked node, and Get(k1) no
// longer found k1. The list reaches the copy of the item in the new array
// now, and the old array is free to go.
func Test_Repro_3_1_1_LinkedItemFreed(t *testing.T) {
	m, k1, node := setPastExpand(t)
	wp, addr := weakItem(node)
	node = nil
	_ = node
	if !weakAlive(wp) {
		t.Fatalf("the weak pointer is nil before GC")
	}
	runtime.GC()
	runtime.GC()
	if wp.Value() != nil {
		return
	}
	if skiplistmap.StepEntryLinked(m.base, addr) {
		t.Errorf("the array of the item of %q (%#x) was freed; the list of entries still reaches it", k1, addr)
	}

	reuseSink = nil
	defer func() { reuseSink = nil }()
	reused := false
	for i := 0; i < 4096 && !reused; i++ {
		a := make([]skiplistmap.SampleItem, skiplistmap.CntOfPersamepleItemPool)
		reuseSink = append(reuseSink, a)
		lo := uintptr(unsafe.Pointer(&a[0]))
		reused = lo <= addr && addr < lo+uintptr(len(a))*skiplistmap.SampleItemSize
	}
	if !reused {
		t.Fatalf("no new array reused the freed memory")
	}
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Errorf("after reuse: %v", err)
	}
	if _, ok := m.Get(k1); !ok {
		t.Errorf("after reuse: Get(%q) not found", k1)
	}
}
