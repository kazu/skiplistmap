//go:build stephook

package skiplistmap_test

import (
	"testing"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Keys z < f0 < f1 < ... share one pool, and z is the smallest key of its
// bucket, so the node before it in the list is the dummy D of the bucket,
// which lies outside the items of the pool. The main goroutine sets f0, which
// takes index 0 of the pool. G2 sets z, takes index 1, which is not the last
// one, and stops at add2.found with the old f0 as its position. The main
// goroutine sets f1 to f62, which take indexes 2 to 63. G1 sets f63, finds
// the pool full and runs _expand, which copies the items, the unlinked z
// among them, into a new array and stops before RepaireSliceAfterCopy. G2
// resumes and links the old z between D and the old f0: D.next is the old z.
// G1 resumes. RepaireSliceAfterCopy skips the old f0, whose links now stay
// inside the array, moves D.next from the old z to the new z, and then finds
// that the new z, copied before it was linked, points to itself. It returns
// an error without putting D.next back, and the Get of G1 panics with
// "already deleted". The list now goes from D to the new z, which points to
// itself, and never reaches f0 or its tail.
//
// Whether RepaireSliceAfterCopy puts the links back or retries, the list must
// not be broken when it fails.
func Test_Repro_3_4_2c_FailedRepairLeavesListBroken(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	kz, fs := keys[0], keys[1:]
	m := newStepMap()
	setKeys(t, m, fs[:1])

	s := newStepper(t)
	stz := s.stopAt("map.add2.found", func(a, b, c unsafe.Pointer) bool {
		return skiplistmap.StepEntryReverse(a) == reverseOf(kz)
	})
	done2 := goStep(t, func() {
		if !m.Set(kz, &list_head.ListHead{}) {
			t.Errorf("Set(%q) failed", kz)
		}
	})
	stz.waitReached(t, done2)
	if r := skiplistmap.StepEntryReverse(stz.b); r != reverseOf(fs[0]) {
		t.Fatalf("Set(%q) stopped before %016x, want f0 %016x", kz, r, reverseOf(fs[0]))
	}
	setKeys(t, m, fs[1:n-1])

	stc := s.stopAt("map.pool.expand.copied", nil)
	var recovered any
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		defer func() { recovered = recover() }()
		m.Set(fs[n-1], &list_head.ListHead{})
	}()
	stc.waitReached(t, done1)
	stz.Release()
	waitDone(t, done2, "Set(z)")
	stc.Release()
	waitDone(t, done1, "Set(f63)")

	if recovered != nil {
		t.Logf("Set(f63) panicked: %v", recovered)
	}
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Fatalf("list broken after _expand (z is %016x): %v", reverseOf(kz), err)
	}
	stored := keys
	if recovered != nil {
		stored = keys[:n] // without f63
	}
	assertStoredInOrder(t, m, stored)
}
