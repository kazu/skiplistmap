//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// J29 (3.4.9): when add2 is given a dummy that is already linked (the loser
// of 2.3 links the dummy of the winner again), its MarkForDelete and Init of
// the dummy make the pending interleaving of 3.4.9 happen inside the map:
// the rollback of a concurrent insertion puts back a link to the dummy, and
// then Init unlinks the dummy, so the previous node points to a node that
// Init unlinked.
//
// Keys: those of startTwoSplits (the lower keys are of 0x30.., b is 0x34..),
// and K of 0x33..: the position of K is just before the dummy D of b.
//
// Interleaving:
//  1. startDummyRace: L (the loser) stops at map.add2.found of D, W (the
//     winner) links D and b and finishes, and K stops at elist.add.cas2
//     after its first CAS: p -> K -> D, D.prev = p.
//  2. L resumes: D is not single, so add2 runs MarkForDelete on D. Both
//     links of D are marked. p.next is K, not D, so MarkForDelete does not
//     change it, and L stops at elist.del.check.
//  3. K resumes: its second CAS expects D.prev to be p, finds the mark, and
//     fails. K stops at elist.add.rollback.
//  4. K resumes: the rollback CAS moves p.next from K back to D. K retries
//     and stops: at its next elist.add.cas1 when insertBefore retries (up
//     to 53f4580), or at map.find.begin when add2 finds the position again
//     (from a7b359c on).
//  5. L resumes: MarkForDelete returns nil, add2 runs Init on D and stops at
//     elist.insert.begin of D, before it links D again.
//
// At that point p.next must not point to a node whose links Init zeroed.
// Before the fix of 2.3 it does: p.next is D, and D is self-linked, so the
// forward chain from p ends at D. After the fix L gets no bucket and leaves
// the split to W; the test then stores K and checks the map.
func Test_ReproJ29LinkedDummyInitAfterRollback(t *testing.T) {
	sp, r := startDummyRace(t, regionKeys(0x3, 0x3, 1), 0)
	if r == nil {
		finishSplitsAfterFix(t, sp)
		k := &sp.extra[0]
		done := goStep(t, func() { sp.m.base.StoreItem(k) })
		sp.stored = append(sp.stored, k.K)
		waitDone(t, done, "StoreItem(K)")
		if !t.Failed() {
			assertStoredInOrder(t, sp.m, sp.stored)
		}
		runtime.KeepAlive(sp.items)
		return
	}
	s := sp.s

	lCheck := s.stopAt("elist.del.check", isNode(r.d))
	r.lFound.Release()
	lCheck.waitReached(t, sp.loseDone)
	t.Logf("L marked D: D is marked: %v; p.next is K: %v", skiplistmap.StepIsMarked(r.d), m24Next(r.p) == r.k)

	kRollback := s.stopAt("elist.add.rollback", isNode(r.k))
	r.kCas2.Release()
	kRollback.waitReached(t, r.kDone)
	// K retries inside insertBefore (up to 53f4580) or from the find of
	// add2 (from a7b359c on); only K runs here.
	kCas1 := s.stopAt("elist.add.cas1", isNode(r.k))
	kFind := s.stopAt("map.find.begin", nil)
	kRollback.Release()
	kRetry := m24ReachedFirst(t, s, kCas1, kFind, r.kDone)
	if kRetry == nil {
		t.Fatalf("StoreItem(K) finished without retrying")
	}
	t.Logf("K rolled back: p.next is D: %v; K retries from %s", m24Next(r.p) == r.d, kRetry.point)

	lInsert := s.stopAt("elist.insert.begin", isNode(r.d))
	lCheck.Release()
	lInsert.waitReached(t, sp.loseDone)

	pNext := m24Next(r.p)
	t.Logf("L ran Init on D: p.next=%p D.prev=%p D.next=%p (p=%p D=%p K=%p)",
		pNext, m24Prev(r.d), m24Next(r.d), r.p, r.d, r.k)
	if pNext == r.d && m24SelfLinked(r.d) {
		t.Errorf("p.next points to D, which Init unlinked (D is self-linked); the forward chain from p ends at D")
	}

	kRetry.Release()
	lInsert.Release()
	t.Logf("K finished: %v, L finished: %v",
		m24Finished(r.kDone, 5*time.Second), m24Finished(sp.loseDone, 5*time.Second))
	runtime.KeepAlive(sp.items)
}
