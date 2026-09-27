//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

// visitOf matches the n-th time a point is reached with p as its first
// argument. The stepper calls a match under its lock, so the count needs no
// other synchronization.
func visitOf(p unsafe.Pointer, n int) func(a, b, c unsafe.Pointer) bool {
	k := 0
	return func(a, _, _ unsafe.Pointer) bool {
		if a != p {
			return false
		}
		k++
		return k >= n
	}
}

// Two goroutines split the same bucket at the same time. The loser of the
// state CAS must not insert the new bucket into the list of buckets: the
// winner does, and a second insertion of a linked bucket breaks the list.
//
// Interleaving: the winner G1 and the loser G2 stop at makeBucket.claimed
// with the new bucket b (reverse 0x34..). Before the fix G2 goes on: the dummy
// D of b is still empty, so G2 initializes b, finds t (reverse 0x30..) as the
// bucket to insert b before, initializes D and stops at add2.found for D.
// G1 then runs to the end: it links D, links b before t and makes b active.
// G3 stores keys between t and b until t splits into d (reverse 0x32..), so
// the list of buckets is b, d, t. G2 resumes: it finds D linked, deletes and
// links it again, and at insertBucket.dummyLinked the entry list is still
// sound. Then G2 inserts b before t once more: it reads d as the prev of t,
// and sets the prev of b to d, the next of d to b and the prev of t to b.
// The prev of d is still b, so b and d are the prev of each other. A later
// split in the range of b walks the list backward from t in makeBucket and
// never leaves b and d.
func Test_ReproA6LoserInsertsLinkedBucketAgain(t *testing.T) {
	dKeys := regionKeys(0x3, 0x1, 32)
	bKeys := regionKeys(0x3, 0x5, 8)
	sp := startTwoSplits(t, append(append([]string{}, dKeys...), bKeys...))
	s, m, b := sp.s, sp.m, sp.b
	dItems, bItems := sp.extra[:len(dKeys)], sp.extra[len(dKeys):]
	if r := skiplistmap.StepBucketReverse(b) >> 56; r != 0x34 {
		t.Fatalf("the new bucket has reverse %02x.., want 34..", r)
	}
	dummy := skiplistmap.StepBucketDummy(b)

	var found *stepStop
	if sp.lose.a == nil {
		// The loser leaves the split to the winner.
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser")
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
	} else {
		found = s.stopAt("map.add2.found", isNode(dummy))
		sp.lose.Release()
		found.waitReached(t, sp.loseDone)
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
	}

	// G3 splits t into d between t and b.
	var d unsafe.Pointer
	for i := range dItems {
		m.base.StoreItem(&dItems[i])
		sp.stored = append(sp.stored, dItems[i].K)
		if a, _ := s.args("map.makeBucket.claimed"); a != nil && skiplistmap.StepBucketReverse(a)>>56 == 0x32 {
			d = a
			break
		}
	}
	if d == nil {
		t.Fatalf("storing %d keys below b did not split the bucket below b", len(dItems))
	}
	if err := skiplistmap.StepCheckBucketsBackward(m.base); err != nil {
		t.Fatalf("before the loser resumes: %v", err)
	}

	if found != nil {
		linked := s.stopAt("map.insertBucket.dummyLinked", isNode(b))
		found.Release()
		linked.waitReached(t, sp.loseDone)
		if err := skiplistmap.StepCheckLists(m.base); err != nil {
			t.Fatalf("after the loser links the dummy again: %v", err)
		}
		linked.Release()
		waitDone(t, sp.loseDone, "loser")
	}

	if err := skiplistmap.StepCheckBucketsBackward(m.base); err != nil {
		t.Errorf("%v", err)
	}

	loop := s.stopAt("map.makeBucket.pairWalk", visitOf(b, 1000))
	done := goStep(t, func() {
		for i := range bItems {
			m.base.StoreItem(&bItems[i])
		}
	})
	select {
	case <-loop.reached:
		t.Fatalf("a split in the range of b walked to b 1000 times: the walk over the list of buckets does not end")
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("storing the keys in the range of b did not finish")
	}
	for i := range bItems {
		sp.stored = append(sp.stored, bItems[i].K)
	}
	if n := s.count("map.insertBucket.begin", b); n != 1 {
		t.Errorf("the new bucket was inserted %d times, want 1", n)
	}
	assertStoredInOrder(t, m, sp.stored)
	runtime.KeepAlive(sp.items)
}
