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
// runs MarkForDelete and Init on D, and then links D again. From that Init
// until D is linked again, D is empty. When the winner checks b.head() in
// that window, b.head() reads the empty D 100 times and makeBucket of the
// winner panics with "bucket head empty".
//
// Keys: those of startTwoSplits (b is 0x34..).
//
// Interleaving: the winner W and the loser L stop at makeBucket.claimed with
// b. L runs Init on D and stops at add2.found of D, before linking it. W
// links D and stops at insertBucket.dummyLinked of b. L resumes: D is not
// single, so L runs MarkForDelete and Init on D and stops at the elist
// insert.begin of D, before linking D again. W resumes: it links b, and
// makeBucket checks b.head(). After the fix L gets no bucket and leaves the
// split to W.
func Test_ReproJ6WinnerReadsDummyBeingRelinked(t *testing.T) {
	sp := startTwoSplits(t, nil)
	s := sp.s
	if sp.lose.a != nil {
		d := skiplistmap.StepBucketDummy(sp.b)
		found := s.stopAt("map.add2.found", isNode(d))
		sp.lose.Release()
		found.waitReached(t, sp.loseDone)
		linked := s.stopAt("map.insertBucket.dummyLinked", isNode(sp.b))
		sp.win.Release()
		linked.waitReached(t, sp.winDone)
		relink := s.stopAt("elist.insert.begin", isNode(d))
		found.Release()
		relink.waitReached(t, sp.loseDone)
		if next := skiplistmap.StepDirectNext(d); next != d {
			relink.Release()
			linked.Release()
			t.Fatalf("D is linked while the loser is about to link it again")
		}
		t.Logf("the loser ran Init on D; the winner checks b.head()")
		linked.Release()
		waitDone(t, sp.winDone, "winner")
		t.Logf("the winner finished; failed: %v; D is linked: %v", t.Failed(), skiplistmap.StepDirectNext(d) != d)
		relink.Release()
		waitDone(t, sp.loseDone, "loser")
	} else {
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser")
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
	}
	if t.Failed() {
		return
	}
	assertStoredInOrder(t, sp.m, sp.stored)
	runtime.KeepAlive(sp.items)
}
