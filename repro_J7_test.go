//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// Two goroutines split the same bucket at the same time. Before the fix the
// loser of the state CAS gets the new bucket b of the winner too, and links
// the dummy D of b again after the winner linked it: add2 finds D linked, and
// runs MarkForDelete and Init on D before linking it again. From
// MarkForDelete until Init, D is marked, and an insertion just before D fails
// with ErrMarked. Before the order check fix add2 then logs "fail insert" and
// returns true, and the key is lost.
//
// Keys: those of startTwoSplits (the lower keys are of 0x30.., b is 0x34..),
// and K of 0x33.., stored during the splits: K comes just before D.
//
// Interleaving: the winner W and the loser L stop at makeBucket.claimed with
// b. L runs Init on D and stops at add2.found of D, before linking it. W runs
// to the end: it links D and b. L resumes: D is not single, so L runs
// MarkForDelete on D and stops at the elist del.marked of D, with D marked.
// K is stored: add2 finds D as the position of K and links K before D. Then L
// resumes, runs Init on D and links D again. After the order check fix add2
// finds the position of K again and retries while D is marked; the test holds
// K at its next add2.found until L finishes. After the fix L gets no bucket
// and leaves the split to W.
func Test_ReproJ7InsertBeforeMarkedDummyIsDropped(t *testing.T) {
	sp := startTwoSplits(t, regionKeys(0x3, 0x3, 1))
	s := sp.s
	k := &sp.extra[0]
	if sp.lose.a != nil {
		d := skiplistmap.StepBucketDummy(sp.b)
		found := s.stopAt("map.add2.found", isNode(d))
		sp.lose.Release()
		found.waitReached(t, sp.loseDone)
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		marked := s.stopAt("elist.del.marked", isNode(d))
		found.Release()
		marked.waitReached(t, sp.loseDone)

		foundK := s.stopAt("map.add2.found", isNode(nodeOf(k)))
		kDone := goStep(t, func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("StoreItem(K) panicked: %v", r)
				}
			}()
			sp.m.base.StoreItem(k)
		})
		sp.stored = append(sp.stored, k.K)
		foundK.waitReached(t, kDone)
		if foundK.b != d {
			foundK.Release()
			marked.Release()
			t.Fatalf("add2 found %016x as the position of K, want D", skiplistmap.StepListReverse(foundK.b))
		}
		foundK.Release()

		// K either finishes, or retries while D is marked.
		deadline := time.Now().Add(10 * time.Second)
		for s.count("map.add2.found", nodeOf(k)) < 2 {
			select {
			case <-kDone:
			case <-time.After(time.Millisecond):
				if time.Now().Before(deadline) {
					continue
				}
				marked.Release()
				t.Fatalf("StoreItem(K) neither finished nor retried while D is marked")
			}
			break
		}
		t.Logf("D is marked: %v; add2 found a position for K %d times and began to link K %d times",
			skiplistmap.StepIsMarked(d), s.count("map.add2.found", nodeOf(k)), s.count("elist.insert.begin", nodeOf(k)))

		// A K that retries waits at its next add2.found until the loser
		// finishes, so that the two do not run at the same time.
		var retryK *stepStop
		select {
		case <-kDone:
		default:
			retryK = s.stopAt("map.add2.found", isNode(nodeOf(k)))
			retryK.waitReached(t, kDone)
		}
		marked.Release()
		waitDone(t, sp.loseDone, "loser")
		if retryK != nil {
			retryK.Release()
		}
		waitDone(t, kDone, "StoreItem(K)")
		runWithDeadline(t, 10*time.Second, func() {
			if _, ok := sp.m.Get(k.K); !ok {
				t.Errorf("K is lost")
			}
		})
	} else {
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser")
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		done := goStep(t, func() { sp.m.base.StoreItem(k) })
		sp.stored = append(sp.stored, k.K)
		waitDone(t, done, "StoreItem(K)")
	}
	if t.Failed() {
		return
	}
	assertStoredInOrder(t, sp.m, sp.stored)
	runtime.KeepAlive(sp.items)
}
