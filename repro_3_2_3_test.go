//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// r323ElistMode returns the traversal mode that elist uses for Next and Prev
// called without options, and leaves it at Direct.
func r323ElistMode() list_head.TraverseType {
	prev := elist_head.SharedTrav(list_head.Direct())
	mt := list_head.NewTraverse()
	prev[0](mt)
	return mt.Type()
}

// The mode is Direct before RangeItem runs over a map with the embedded pool
// and nothing runs concurrently. RangeItem calls Next and Prev of elist with
// WaitNoM(), which elist stores into the traversal mode shared by the whole
// process and never puts back; RangeItem puts back only the mode of lista.
// After RangeItem returns, Next and Prev of elist without options still wait
// for no mark.
func Test_Repro_3_2_3_RangeItemLeavesWaitNoMark(t *testing.T) {
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	for i := 0; i < 8; i++ {
		if !m.Set(crashKey(i), &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", crashKey(i))
		}
	}
	if mode := r323ElistMode(); mode != list_head.TravDirect {
		t.Fatalf("mode of elist before RangeItem = %v, want TravDirect", mode)
	}
	m.base.RangeItem(func(skiplistmap.MapItem) bool { return true })
	if mode := r323ElistMode(); mode != list_head.TravDirect {
		t.Errorf("mode of elist after RangeItem = %v (TravWaitNoMark = %v), want TravDirect %v", mode, list_head.TravWaitNoMark, list_head.TravDirect)
	}
}

// r323PanicAt returns the innermost function of package skiplistmap on the
// stack of a panicking goroutine. Call it from the deferred function that
// recovers.
func r323PanicAt() string {
	pc := make([]uintptr, 64)
	frames := runtime.CallersFrames(pc[:runtime.Callers(2, pc)])
	for {
		f, more := frames.Next()
		if strings.HasPrefix(f.Function, "github.com/kazu/skiplistmap.") {
			return fmt.Sprintf("%s (%s:%d)", f.Function, filepath.Base(f.File), f.Line)
		}
		if !more {
			return "an unknown function"
		}
	}
}

// r323Map is a map with the embedded pool and at most 16 items per bucket.
// Bucket L holds the keys whose reversed hashes have the top 4 bits 3 and
// bucket S those with 4, so the list of entries holds the dummy of L, the
// keys of L, then the dummy of S. d is the last key of L, and b is a key not
// stored that comes after d in L. S holds exactly 16 keys, and y is a key not
// stored in S, whose Set splits S.
type r323Map struct {
	m      *WrapHMap
	stored []string
	d, b   string
	y      string
}

func newR323Map(t *testing.T) *r323Map {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	// candL holds the keys of L; the first 4 in the order of their reversed
	// hashes are stored, and the fifth is b.
	var candL, inS, restS []string
	for i := 0; len(candL) < 5 || len(restS) < 1; i++ {
		k := crashKey(i)
		switch reverseOf(k) >> 60 {
		case 3:
			candL = append(candL, k)
		case 4:
			if len(inS) < 16 {
				inS = append(inS, k)
			} else {
				restS = append(restS, k)
			}
		}
	}
	sort.Slice(candL, func(i, j int) bool { return reverseOf(candL[i]) < reverseOf(candL[j]) })
	d, b := candL[3], candL[4]
	stored := append(append([]string{}, candL[:4]...), inS...)
	for _, k := range stored {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	foundL, baseL := skiplistmap.StepLockBuckets(m.base, reverseOf(d))
	foundS, baseS := skiplistmap.StepLockBuckets(m.base, reverseOf(inS[0]))
	if foundL != baseL || foundS != baseS || foundL == foundS {
		t.Fatalf("buckets of L and S are not two buckets that own their pools")
	}
	for _, k := range append(append([]string{}, stored...), b, restS[0]) {
		want := foundL
		if reverseOf(k)>>60 == 4 {
			want = foundS
		}
		if f, _ := skiplistmap.StepLockBuckets(m.base, reverseOf(k)); f != want {
			t.Fatalf("key %q is not in the bucket of the top 4 bits of its reversed hash", k)
		}
	}
	return &r323Map{m: m, stored: stored, d: d, b: b, y: restS[0]}
}

// splitNextToMarkedEntry runs the schedule below and checks the keys.
//
// d is marked deleted, but stays linked. G1 sets b. Set locks the muPool of L
// and reuses the slot of d, which getWithFn unlinks with MarkForDelete. G1
// stops after MarkForDelete has marked the links of d and before it relinks
// the neighbours, so the dummy of S still points back at the marked d. G2
// then sets y. Set locks the muPool of S, which G1 does not hold, adds y and
// splits S, since S holds more than 16 keys. The split links the dummy of the
// new bucket before S, and _InsertBefore starts from the entry before the
// dummy of S: tBucket.head().Prev().Next() with no options, which reads the
// traversal mode of elist shared by the whole process. In Direct, Prev()
// returns d; in WaitNoMark, Prev() waits for the mark on d to go, gives up
// and returns nil, and Next() on nil panics.
func splitNextToMarkedEntry(t *testing.T, p *r323Map) {
	t.Helper()
	m := p.m
	itemD, ok := m.base.LoadItem(p.d)
	if !ok {
		t.Fatalf("LoadItem(%q) not found", p.d)
	}
	nodeD := unsafe.Pointer(itemD.PtrListHead())
	if !m.base.Delete(p.d) {
		t.Fatalf("Delete(%q) failed", p.d)
	}

	s := newStepper(t)
	st := s.stopAt("elist.del.marked", isNode(nodeD))
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.Set(p.b, &list_head.ListHead{}) })
	st.waitReached(t, done1)

	done2 := goStep(t, func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Set(%q) panicked in %s", p.y, r323PanicAt())
				panic(r)
			}
		}()
		ok2 = m.Set(p.y, &list_head.ListHead{})
	})
	waitDone(t, done2, "Set of the key that splits S")
	if n := s.total("map.insertBucket.begin"); n < 1 {
		t.Errorf("Set(%q) did not split S", p.y)
	}

	st.Release()
	waitDone(t, done1, "Set of the key that reuses the slot of d")
	if !ok1 || !ok2 {
		t.Errorf("Set(%q) = %v, Set(%q) = %v, want both true", p.b, ok1, p.y, ok2)
	}

	var want []string
	for _, k := range p.stored {
		if k != p.d {
			want = append(want, k)
		}
	}
	want = append(want, p.b, p.y)
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Errorf("%v", err)
		return
	}
	assertStoredInOrder(t, m, want)
}

// After RangeItem, the traversal mode of elist is WaitNoMark (see
// Test_Repro_3_2_3_RangeItemLeavesWaitNoMark), and the split in G2 panics
// with a nil pointer dereference in _InsertBefore.
func Test_Repro_3_2_3_SplitPanicsAfterRangeItem(t *testing.T) {
	p := newR323Map(t)
	p.m.base.RangeItem(func(skiplistmap.MapItem) bool { return true })
	splitNextToMarkedEntry(t, p)
}

// The same schedule in Direct: G2 passes the marked d, both Sets finish and
// the map holds the keys. The panic above comes from the mode that RangeItem
// left behind.
func Test_Repro_3_2_3_SplitWithDirectMode(t *testing.T) {
	p := newR323Map(t)
	elist_head.SharedTrav(list_head.Direct())
	splitNextToMarkedEntry(t, p)
}
