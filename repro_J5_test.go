//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// Two goroutines split the same bucket at the same time. Before the fix the
// loser of the state CAS gets the new bucket b of the winner too, and links
// the dummy D of b again after the winner linked it: add2 finds D linked,
// runs MarkForDelete and Init on D, and links D before the position it found
// earlier. When that position is stale, the order check of add2 rejects D.
// Before the order check fix add2 then logs "fail insert" and returns true,
// D stays unlinked, and makeBucket of the loser panics with "bucket head
// empty".
//
// Keys: those of startTwoSplits (b is 0x34..; no key lies between 0x34.. and
// the next bucket 0x38..), and K of 0x35.., stored during the splits.
//
// Interleaving: the winner W and the loser L stop at makeBucket.claimed with
// b. L runs Init on D, finds the position P of D (the first entry above D,
// with D not linked), and stops at add2.found of D. W runs to the end: it
// links D just before P, and links b. K is stored: it goes into b, between
// D and P, and its store splits b at 0x36.., whose dummy comes between K and
// P. L resumes: D is not single, so L runs MarkForDelete and Init on D and
// links D before P, where the entry before P is now the dummy of 0x36..,
// above D. After the fix L gets no bucket and leaves the split to W.
func Test_ReproJ5LoserRelinksDummyAtStalePosition(t *testing.T) {
	sp := startTwoSplits(t, regionKeys(0x3, 0x5, 1))
	s := sp.s
	k := &sp.extra[0]
	store := func() {
		done := goStep(t, func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("StoreItem(K) panicked: %v", r)
				}
			}()
			sp.m.base.StoreItem(k)
		})
		sp.stored = append(sp.stored, k.K)
		waitDone(t, done, "StoreItem(K)")
	}
	if sp.lose.a != nil {
		d := skiplistmap.StepBucketDummy(sp.b)
		found := s.stopAt("map.add2.found", isNode(d))
		sp.lose.Release()
		found.waitReached(t, sp.loseDone)
		p := found.b
		t.Logf("the loser found %016x as the position of D", skiplistmap.StepListReverse(p))
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		if next := skiplistmap.StepDirectNext(d); next != p {
			found.Release()
			t.Fatalf("the winner linked D before %016x, want the position the loser found", skiplistmap.StepListReverse(next))
		}
		store()
		prev := skiplistmap.StepDirectPrev(p)
		t.Logf("K is linked; the entry before the position the loser found is %016x", skiplistmap.StepListReverse(prev))
		if skiplistmap.StepListReverse(prev) <= skiplistmap.StepBucketReverse(sp.b) {
			found.Release()
			t.Fatalf("the entry before the position the loser found is not above D")
		}
		found.Release()
		waitDone(t, sp.loseDone, "loser")
		t.Logf("add2 found a position for D %d times; D is linked: %v", s.count("map.add2.found", d), skiplistmap.StepDirectNext(d) != d)
	} else {
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser")
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		store()
	}
	if t.Failed() {
		return
	}
	assertStoredInOrder(t, sp.m, sp.stored)
	runtime.KeepAlive(sp.items)
}
