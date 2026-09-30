//go:build stephook

package skiplistmap_test

import (
	"runtime/debug"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// The helpers below are written with what the trees before the fixes also
// have, so that the J27 to J32 tests build there too.

// m24Next returns the next of the list node p without skipping marked nodes.
func m24Next(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer((*elist_head.ListHead)(p).DirectNext())
}

// m24Prev returns the prev of the list node p without skipping marked nodes.
func m24Prev(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer((*elist_head.ListHead)(p).DirectPrev())
}

// m24SelfLinked reports whether both links of the list node p point to p,
// which is what Init leaves.
func m24SelfLinked(p unsafe.Pointer) bool {
	return m24Next(p) == p && m24Prev(p) == p
}

// m24Reached waits until a goroutine stops at st or done is closed, and
// reports whether it stopped. It fails the test if neither happens in 10
// seconds.
func m24Reached(t *testing.T, st *stepStop, done <-chan struct{}) bool {
	t.Helper()
	select {
	case <-st.reached:
		return true
	case <-done:
		return false
	case <-time.After(10 * time.Second):
		t.Fatalf("no goroutine reached %s", st.point)
		return false
	}
}

// m24ReachedFirst waits until a goroutine stops at one of a and b of s, and
// returns it, or nil when done is closed first. The other stop is disabled,
// so that it stops no goroutine later. It fails the test if neither happens
// in 10 seconds.
func m24ReachedFirst(t *testing.T, s *stepper, a, b *stepStop, done <-chan struct{}) (got *stepStop) {
	t.Helper()
	defer func() {
		s.mu.Lock()
		a.used, b.used = true, true
		s.mu.Unlock()
	}()
	select {
	case <-a.reached:
		return a
	case <-b.reached:
		return b
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		t.Fatalf("no goroutine reached %s or %s", a.point, b.point)
		return nil
	}
}

// m24Finished waits up to d for done and reports whether it was closed.
func m24Finished(done <-chan struct{}, d time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// m24HoldGC keeps the collector off for the test, so that the old array of a
// pool that _expand drops stays in place while nodes of the list still point
// into it.
func m24HoldGC(t *testing.T) {
	t.Helper()
	old := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(old) })
}

// m24CheckStored checks that the lists of m are sound and, only then (a
// lookup on a broken list may not end), that every key is found.
func m24CheckStored(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](m.base); err != nil {
		t.Errorf("%v", err)
		return
	}
	runWithDeadline(t, 10*time.Second, func() {
		for _, k := range keys {
			if _, ok := m.Get(k); !ok {
				t.Errorf("Get(%q) not found", k)
			}
		}
	})
}

// dummyRace holds the two splits of startTwoSplits after the loser L was
// given the bucket b of the winner W (before the fix of 2.3), with:
//   - L stopped at map.add2.found of the dummy D of b: L ran Init on D and
//     found the position of D, before it looks at D again;
//   - W run to its end: W linked D and b;
//   - K, an extra key just before D, stopped at elist.add.cas2: its first
//     CAS moved p.next from D to K, and its second CAS (D.prev from p to K)
//     has not run.
type dummyRace struct {
	*twoSplits
	d, p, k unsafe.Pointer // the nodes of D, of p (the prev of D), and of K
	lFound  *stepStop      // L at map.add2.found of D
	kCas2   *stepStop      // K at elist.add.cas2
	kDone   <-chan struct{}
}

// startDummyRace runs the steps of dummyRace, storing the extra key at
// index k as K. It returns nil when L got no bucket (after the fix of 2.3).
func startDummyRace(t *testing.T, extra []string, k int) (*twoSplits, *dummyRace) {
	t.Helper()
	sp := startTwoSplits(t, extra)
	if sp.lose.a == nil {
		return sp, nil
	}
	r := &dummyRace{twoSplits: sp}
	s := sp.s
	r.d = skiplistmap.StepBucketDummy(sp.b)
	r.lFound = s.stopAt("map.add2.found", isNode(r.d))
	sp.lose.Release()
	r.lFound.waitReached(t, sp.loseDone)
	sp.win.Release()
	waitDone(t, sp.winDone, "winner")
	if n := m24Next(m24Prev(r.d)); n != r.d {
		t.Fatalf("D is not linked after the winner finished: the next of its prev is %p, want D %p", n, r.d)
	}

	item := &sp.extra[k]
	r.k = nodeOf(item)
	r.kCas2 = s.stopAt("elist.add.cas2", isNode(r.k))
	r.kDone = goStep(t, func() { sp.m.base.StoreItem(item) })
	sp.stored = append(sp.stored, string(item.Key()))
	r.kCas2.waitReached(t, r.kDone)
	r.p = r.kCas2.b
	if m24Next(r.k) != r.d || m24Next(r.p) != r.k || m24Prev(r.d) != r.p {
		t.Fatalf("K is not half linked before D: p.next=%p K.next=%p D.prev=%p (p=%p K=%p D=%p)",
			m24Next(r.p), m24Next(r.k), m24Prev(r.d), r.p, r.k, r.d)
	}
	t.Logf("K (%016x) stopped between its two CASes: p (%016x) -> K -> D (%016x), D.prev = p",
		skiplistmap.StepListReverse(r.k), skiplistmap.StepListReverse(r.p), skiplistmap.StepListReverse(r.d))
	return sp, r
}

// finishSplitsAfterFix runs L and W of startTwoSplits to their ends, in
// that order, when L got no bucket.
func finishSplitsAfterFix(t *testing.T, sp *twoSplits) {
	t.Helper()
	sp.lose.Release()
	waitDone(t, sp.loseDone, "loser")
	sp.win.Release()
	waitDone(t, sp.winDone, "winner")
}
