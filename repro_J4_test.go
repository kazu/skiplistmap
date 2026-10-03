//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// Two goroutines split the same bucket at the same time. Before the fix the
// loser of the state CAS gets the new bucket b of the winner too, and runs
// b.Init() on it. When that Init runs after the winner linked b into the
// list of buckets, it gives b a new end of the list: the bucket above b still
// points to b, but a walk from the head of the list stops after b and does
// not reach the buckets below b. A later split below b then links nothing in
// addBucket, and makeBucket panics with "bucket head empty".
//
// Keys: those of startTwoSplits (b is 0x34..), and keys of 0x31.. stored
// after the splits.
//
// Interleaving: the winner W and the loser L stop at makeBucket.claimed with
// b. L passes the check that the dummy of b is empty, and stops at
// makeBucket.beforeInit of b. W runs to the end: it links the dummy of b and
// b. L resumes, runs b.Init() and b.LevelHead.Init(), and finishes. Then the
// keys of 0x31.. are stored one by one; one of them splits the bucket below
// b at 0x32.., and addBucket walks from the head of the list of buckets to
// find the place of the new bucket below b. After the fix L gets no bucket
// and leaves the split to W.
func Test_ReproJ4LoserInitsLinkedBucket(t *testing.T) {
	sp := startTwoSplits(t, regionKeys(0x3, 0x1, 8))
	s := sp.s
	if sp.lose.a != nil {
		beforeInit := s.stopAt("map.makeBucket.beforeInit", isNode(sp.b))
		sp.lose.Release()
		beforeInit.waitReached(t, sp.loseDone)
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		t.Logf("the winner linked b; the loser runs b.Init()")
		beforeInit.Release()
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
	rb := skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](sp.b)
	rs := skiplistmap.StepBucketsForward(sp.m.base, 1000)
	if last := rs[len(rs)-1]; last >= rb {
		t.Errorf("a walk from the head of the list of buckets reaches %d buckets and stops at %016x, not below b %016x", len(rs), last, rb)
	}

	claimed := s.total("map.makeBucket.claimed")
	for i := range sp.extra {
		it := &sp.extra[i]
		var r any
		done := goStep(t, func() {
			defer func() {
				if r = recover(); r != nil {
					t.Errorf("StoreItem(%q) panicked: %v", string(it.Key()), r)
				}
			}()
			sp.m.base.StoreItem(it)
		})
		sp.stored = append(sp.stored, string(it.Key()))
		waitDone(t, done, "StoreItem("+string(it.Key())+")")
		if r != nil {
			if c, _ := s.args("map.makeBucket.claimed"); c != nil {
				t.Logf("the last split: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](c))
			}
			return
		}
	}
	if n := s.total("map.makeBucket.claimed") - claimed; n == 0 {
		t.Fatalf("the stores of 0x31.. split no bucket below b")
	}
	assertStoredInOrder(t, sp.m, sp.stored)
	runtime.KeepAlive(sp.items)
}
