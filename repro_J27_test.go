//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// J27 (3.4.9): InsertBefore returns ErrMarked at its entrance when the node
// to insert before is marked, and add2 drops that error: the key is not
// linked, and _set goes on as if it were (it counts the key and returns
// true).
//
// Keys: those of startTwoSplits (the lower keys are of 0x30.., b is 0x34..),
// and K of 0x33..: the position of K is just before the dummy D of b.
//
// Interleaving (that of J7, which the 2.3 scene gives):
//  1. The winner W and the loser L of the split stop at makeBucket.claimed
//     with b. L runs Init on D and stops at map.add2.found of D. W runs to
//     the end: it links D and b.
//  2. L resumes: D is not single, so add2 runs MarkForDelete on D. L stops at
//     elist.del.marked of D, with both links of D marked.
//  3. K is stored: add2 finds D as the position of K and stops at
//     map.add2.found; the test checks the position and lets K go on. K
//     passes the order checks (K is single, D is above K, the prev of D is
//     below K) and calls D.InsertBefore(K), which sees the mark of D and
//     returns ErrMarked before elist.insert.begin.
//  4. The test waits until K either returns from add2 and stops at
//     map.makeBucket.begin (_set has counted K), or finds its position again
//     (the fix of 2.1) and stops at its next map.add2.found.
//
// Before the fix of 2.1 add2 drops the error: K never reached
// elist.insert.begin, its links are still those of Init, and _set goes on to
// makeBucket. After the fix of 2.1 add2 finds the position again, and the
// test lets L finish before K (as J7 does). After the fix of 2.3 L gets no
// bucket and leaves the split to W; the test then stores K and checks the
// map.
func Test_ReproJ27InsertBeforeMarkedIsDroppedByAdd2(t *testing.T) {
	sp := startTwoSplits(t, regionKeys(0x3, 0x3, 1))
	s := sp.s
	item := &sp.extra[0]
	k := nodeOf(item)
	if sp.lose.a == nil {
		finishSplitsAfterFix(t, sp)
		done := goStep(t, func() { sp.m.base.StoreItem(item) })
		sp.stored = append(sp.stored, item.K)
		waitDone(t, done, "StoreItem(K)")
		if !t.Failed() {
			assertStoredInOrder(t, sp.m, sp.stored)
		}
		runtime.KeepAlive(sp.items)
		return
	}

	d := skiplistmap.StepBucketDummy(sp.b)
	lFound := s.stopAt("map.add2.found", isNode(d))
	sp.lose.Release()
	lFound.waitReached(t, sp.loseDone)
	sp.win.Release()
	waitDone(t, sp.winDone, "winner")
	lMarked := s.stopAt("elist.del.marked", isNode(d))
	lFound.Release()
	lMarked.waitReached(t, sp.loseDone)

	kFound := s.stopAt("map.add2.found", isNode(k))
	kDone := goStep(t, func() { sp.m.base.StoreItem(item) })
	sp.stored = append(sp.stored, item.K)
	kFound.waitReached(t, kDone)
	if kFound.b != d {
		kFound.Release()
		lMarked.Release()
		t.Fatalf("add2 found %016x as the position of K, want D", skiplistmap.StepListReverse(kFound.b))
	}
	kSplit := s.stopAt("map.makeBucket.begin", isNode(k))
	kAgain := s.stopAt("map.add2.found", isNode(k))
	kFound.Release()
	got := m24ReachedFirst(t, s, kSplit, kAgain, kDone)
	begun := s.count("elist.insert.begin", k)
	t.Logf("D is marked: %v; add2 of K began to link K %d times; K is self-linked: %v",
		skiplistmap.StepIsMarked(d), begun, m24SelfLinked(k))
	switch got {
	case kSplit:
		if begun == 0 && m24SelfLinked(k) {
			t.Errorf("add2 returned without linking K: InsertBefore rejected K at its entrance (D is marked), and _set went on to count K")
		}
		kSplit.Release()
		lMarked.Release()
	case kAgain:
		t.Logf("add2 finds the position of K again")
		lMarked.Release()
		waitDone(t, sp.loseDone, "loser")
		kAgain.Release()
	default:
		lMarked.Release()
		t.Errorf("StoreItem(K) finished without reaching makeBucket or finding its position again")
	}
	t.Logf("K finished: %v, L finished: %v",
		m24Finished(kDone, 5*time.Second), m24Finished(sp.loseDone, 5*time.Second))
	if !t.Failed() {
		assertStoredInOrder(t, sp.m, sp.stored)
	}
	runtime.KeepAlive(sp.items)
}
