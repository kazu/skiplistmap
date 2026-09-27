//go:build stephook

package skiplistmap_test

import (
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// r322Map is a map with the embedded pool whose bucket of the keys has split
// once. prev and x are adjacent stored keys whose lookups find downLevels[0]
// of the bucket (firstDown), which uses the pool of its parent. n is a key not
// stored, between prev and x, in the same pool.
type r322Map struct {
	m       *WrapHMap
	stored  []string
	prev, x string
	n       string
}

func newR322Map(t *testing.T) *r322Map {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	var keys, rest []string
	for i := 0; len(keys)+len(rest) < 1000; i++ {
		k := crashKey(i)
		if reverseOf(k)>>60 != 3 {
			continue
		}
		if len(keys) < 40 {
			keys = append(keys, k)
		} else {
			rest = append(rest, k)
		}
	}
	for _, k := range keys {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })

	for i := 0; i+1 < len(keys); i++ {
		prev, x := keys[i], keys[i+1]
		foundX, baseX := skiplistmap.StepLockBuckets(m.base, reverseOf(x))
		if foundX == baseX {
			continue
		}
		if foundP, _ := skiplistmap.StepLockBuckets(m.base, reverseOf(prev)); foundP != foundX {
			continue
		}
		for _, n := range rest {
			if reverseOf(prev) < reverseOf(n) && reverseOf(n) < reverseOf(x) {
				if _, baseN := skiplistmap.StepLockBuckets(m.base, reverseOf(n)); baseN == baseX {
					return &r322Map{m: m, stored: keys, prev: prev, x: x, n: n}
				}
			}
		}
	}
	t.Fatalf("no adjacent stored keys in firstDown with a free key between them")
	return nil
}

// Keys prev < n < x. prev and x are stored, and lookups of both find
// downLevels[0] of their bucket (firstDown), which uses the pool of its parent.
// n is not stored and belongs to the same pool.
//
// G1 sets n. Set of a new key locks the muPool of the parent, takes a slot
// for n and links it before x. It stops in listAddWitCas after the first CAS
// (prev.next: x -> n) and before the second (x.prev: prev -> n), so n.next is
// x while x.prev is still prev. G2 then purges x. Purge locks firstDown.muPool,
// which G1 does not hold, so G2 does not wait. MarkForDelete(x) finds prev.next
// already pointing at n and x.next.prev pointing back at x, relinks the latter,
// passes its check because neither prev.next nor next.prev points at x any
// more, and returns nil although n.next still points at x. purgeInEmbedded
// ignores the result and runs Init on x. When G1 resumes, its second CAS fails
// on the self-linked x and the rollback puts prev.next back to x.
//
// Both Set(n) and Purge(x) return true, yet n is not linked and the list of
// entries stops at the self-linked x, so a walk misses every key after x.
//
// G2 also stops just before Init, and the test logs what purgeInEmbedded
// could check there: IsSafety of x and whether n still points at x.
func Test_Repro_3_2_2_PurgeBesideUnfinishedInsert(t *testing.T) {
	p := newR322Map(t)
	m := p.m
	revN, revX := reverseOf(p.n), reverseOf(p.x)

	s := newStepper(t)
	st := s.stopAt("elist.add.cas2", func(a, b, c unsafe.Pointer) bool {
		return skiplistmap.StepEntryReverse(a) == revN && skiplistmap.StepEntryReverse(c) == revX
	})
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.Set(p.n, &list_head.ListHead{}) })
	st.waitReached(t, done1)

	st2 := s.stopAt("map.purge.beforeInit", func(a, b, c unsafe.Pointer) bool {
		return skiplistmap.StepEntryReverse(a) == revX
	})
	done2 := goStep(t, func() { ok2 = m.Delete(p.x) })
	select {
	case <-st2.reached:
		// What purgeInEmbedded could check before Init: x is unlinked from
		// prev and from its next, and IsSafety holds, yet n still points at x.
		nodeN, nodeX := (*elist_head.ListHead)(st.a), (*elist_head.ListHead)(st2.a)
		safe, err := nodeX.IsSafety()
		t.Logf("Purge(%q) is about to Init while Set(%q) is stopped between its two CASes: IsSafety() = %v, %v; n.next == x: %v",
			p.x, p.n, safe, err, nodeN.DirectNext() == nodeX)
		st2.Release()
		waitDone(t, done2, "Purge")
	case <-done2:
		t.Logf("Purge(%q) finished without reaching Init", p.x)
	case <-time.After(5 * time.Second):
		t.Logf("Purge(%q) waited for Set(%q)", p.x, p.n)
	}

	st.Release()
	waitDone(t, done1, "Set")
	waitDone(t, done2, "Purge")

	if !ok1 || !ok2 {
		t.Fatalf("Set(%q) = %v, Purge(%q) = %v, want both true", p.n, ok1, p.x, ok2)
	}
	if _, ok := m.Get(p.x); ok {
		t.Errorf("Get(%q) found the purged key", p.x)
	}
	var want []string
	for _, k := range p.stored {
		if k != p.x {
			want = append(want, k)
		}
	}
	want = append(want, p.n)
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		// RangeItem does not end on a self-linked node, so check the rest
		// without it.
		t.Errorf("%v (the purged %q has reverse %016x)", err, p.x, revX)
		for _, k := range want {
			if _, ok := m.Get(k); !ok {
				t.Errorf("Get(%q) not found", k)
			}
		}
		if n := m.base.Len(); n != len(want) {
			t.Errorf("Len() = %d, want %d", n, len(want))
		}
		return
	}
	assertStoredInOrder(t, m, want)
}
