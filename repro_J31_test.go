//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// J31 (3.4.12): the rollback CAS of an insertion can fail, and when the
// insertion then retries with the same node and succeeds, in the scene of
// 2.3 the new node and the node after it link only to each other.
//
// Keys: those of startTwoSplits (the lower keys are of 0x30.., b is 0x34..),
// and M < K of 0x33..: the position of K is just before the dummy D of b,
// and M comes between p (the prev of D) and K.
//
// Interleaving:
//  1. startDummyRace: L (the loser) stops at map.add2.found of D, W (the
//     winner) links D and b and finishes, and K stops at elist.add.cas2
//     after its first CAS: p -> K -> D, D.prev = p.
//  2. L resumes: D is not single, so add2 runs MarkForDelete on D (p.next is
//     K, not D, so it is not changed) and Init on D, and stops at
//     elist.insert.begin of D, before it links D again. D is self-linked.
//  3. K resumes: its second CAS expects D.prev to be p, finds D itself, and
//     fails. K stops at elist.add.rollback.
//  4. M is stored: it finds K as its position from p, reads p as the prev of
//     K, and links M: p.next from K to M, K.prev from p to M. M stops at
//     map.makeBucket.begin, after it is linked.
//  5. K resumes: the rollback CAS expects p.next to be K, finds M, and fails.
//     The rollback puts the links of K back to what they were before the
//     insertion (self-linked, from the Init of _set). K retries with the same
//     node: the prev of D is D itself, so both CASes succeed on D, and K
//     returns. K stops at map.makeBucket.begin.
//
// At that point the list must not have D and K linked only to each other.
// Up to 53f4580, where insertBefore retries with the same node, they are:
// D.next = K, K.next = D, D.prev = K, K.prev = D, and M, reached from p,
// points into the ring. From a7b359c on, add2 retries from its find instead
// of insertBefore, and step 5 ends differently: StoreItem(K) returns with K
// self-linked while M points to it; the test reports that too. After the fix of 2.3 L gets no bucket and leaves the
// split to W; the test then stores M and K and checks the map.
func Test_ReproJ31RollbackFailsAndRetryRingsWithNext(t *testing.T) {
	sp, r := startDummyRace(t, regionKeys(0x3, 0x3, 2), 1)
	if r == nil {
		finishSplitsAfterFix(t, sp)
		for i := range sp.extra {
			it := &sp.extra[i]
			done := goStep(t, func() { sp.m.base.StoreItem(it) })
			sp.stored = append(sp.stored, it.K)
			waitDone(t, done, "StoreItem("+it.K+")")
		}
		if !t.Failed() {
			assertStoredInOrder(t, sp.m, sp.stored)
		}
		runtime.KeepAlive(sp.items)
		return
	}
	s := sp.s
	mItem := &sp.extra[0]
	m := nodeOf(mItem)

	lInsert := s.stopAt("elist.insert.begin", isNode(r.d))
	r.lFound.Release()
	lInsert.waitReached(t, sp.loseDone)
	t.Logf("L ran Init on D: D is self-linked: %v; p.next is K: %v", m24SelfLinked(r.d), m24Next(r.p) == r.k)

	kRollback := s.stopAt("elist.add.rollback", isNode(r.k))
	r.kCas2.Release()
	kRollback.waitReached(t, r.kDone)

	mBegin := s.stopAt("map.makeBucket.begin", isNode(m))
	mDone := goStep(t, func() { sp.m.base.StoreItem(mItem) })
	sp.stored = append(sp.stored, mItem.K)
	mStopped := m24Reached(t, mBegin, mDone)
	t.Logf("M (%016x) stored (stopped before its split: %v): p.next is M: %v, M.next is K: %v, K.prev is M: %v",
		skiplistmap.StepListReverse(m), mStopped, m24Next(r.p) == m, m24Next(m) == r.k, m24Prev(r.k) == m)

	kBegin := s.stopAt("map.makeBucket.begin", isNode(r.k))
	kRollback.Release()
	kStopped := m24Reached(t, kBegin, r.kDone)

	dNext, dPrev, kNext, kPrev := m24Next(r.d), m24Prev(r.d), m24Next(r.k), m24Prev(r.k)
	t.Logf("K returned (stopped before its split: %v): p.next=%p M.next=%p K.prev=%p K.next=%p D.prev=%p D.next=%p (p=%p M=%p K=%p D=%p)",
		kStopped, m24Next(r.p), m24Next(m), kPrev, kNext, dPrev, dNext, r.p, m, r.k, r.d)
	if dNext == r.k && kNext == r.d && dPrev == r.k && kPrev == r.d {
		t.Errorf("D and K link only to each other; M, reached from p, points into the ring, and the forward chain never reaches the tail")
	} else if m24Next(m) == r.k && m24SelfLinked(r.k) {
		t.Errorf("K returned self-linked; M, reached from p, points to K, and the forward chain from p ends at K")
	}

	mBegin.Release()
	kBegin.Release()
	lInsert.Release()
	t.Logf("M finished: %v, K finished: %v, L finished: %v",
		m24Finished(mDone, 5*time.Second), m24Finished(r.kDone, 5*time.Second), m24Finished(sp.loseDone, 5*time.Second))
	runtime.KeepAlive(sp.items)
}
