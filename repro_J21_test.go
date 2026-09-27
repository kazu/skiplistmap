//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// j21Splits holds the map of r325Map after two splits of the same bucket b
// (0x38) linked the same new bucket e (0x3c) into the list of buckets, and
// G1 stopped at "makeBucket2.added" right after its addBucket(e) returned.
type j21Splits struct {
	p     *r325Map
	s     *stepper
	e     unsafe.Pointer
	added *stepStop
	done1 <-chan struct{}
}

// startJ21Splits runs the schedule below on r325Map. G1 (Set of p.low) holds
// the muPool of T, G2 (Set of u4) holds the muPool of b, so nothing orders
// their splits of b (3.2.5).
//
//  1. G1 splits T at 0x38 into T and b, publishes b and stops at
//     "makeBucket2.recurse" before it checks b against the limit.
//  2. G2 stores u4 into b and splits b: it reads the bucket above b, 0x40,
//     computes 0x3c, claims element 12 of the downLevels of T as e and stops
//     at "makeBucket2.got".
//  3. G1 resumes: b is over the limit, so it splits b too. It reads the same
//     bucket above b, computes the same 0x3c, and bucketFromPoolEmbedded
//     returns the same e through the path for an element already made. G1
//     stops at "makeBucket2.got" with e.
//  4. G2 resumes: it runs Init on e, copies u2..u4 into the pool of e, walks
//     the list of buckets in addBucket to the first bucket below 0x3c, b, and
//     stops at "insertBucket.begin", before it links anything.
//  5. G1 resumes and does the same: its walk does not see e either, because
//     G2 has not linked it, and finds b. It stops at "insertBucket.begin".
//  6. G2 runs to its end: it links e before b (0x40, 0x3c, 0x38) and returns.
//  7. G1 resumes: it links e before b again. The insertion reads the prev of
//     b, now e itself, and does not check the order again: its first CAS
//     changes e.next from b to e, its second b.prev from e to e. e is linked
//     to itself, and a walk from the head of the list of buckets goes around
//     e and never reaches b. G1 stops at "makeBucket2.added", after its
//     addBucket(e) returned.
func startJ21Splits(t *testing.T) *j21Splits {
	t.Helper()
	p := newR325Map(t)
	s := newStepper(t)

	recurse := s.stopAt("map.makeBucket2.recurse", nil)
	done1 := goStep(t, func() { p.m.Set(p.low, &list_head.ListHead{}) })
	recurse.waitReached(t, done1)
	b := recurse.b
	if r := skiplistmap.StepBucketReverse(b); r != 0x38<<56 {
		t.Fatalf("G1 split T into a bucket of reverse %016x, want %016x", r, uint64(0x38<<56))
	}

	got2 := s.stopAt("map.makeBucket2.got", isSecond(b))
	done2 := goStep(t, func() { p.m.Set(p.u[4], &list_head.ListHead{}) })
	got2.waitReached(t, done2)
	e := got2.a
	if r := skiplistmap.StepBucketReverse(e); r != p.split {
		t.Fatalf("G2 got a bucket of reverse %016x, want %016x", r, p.split)
	}

	got1 := s.stopAt("map.makeBucket2.got", isSecond(b))
	recurse.Release()
	got1.waitReached(t, done1)
	if got1.a != e {
		t.Fatalf("G1 got the bucket %016x, want the one G2 got (%016x)", skiplistmap.StepBucketReverse(got1.a), p.split)
	}

	walked2 := s.stopAt("map.insertBucket.begin", isNode(e))
	got2.Release()
	walked2.waitReached(t, done2)

	walked1 := s.stopAt("map.insertBucket.begin", isNode(e))
	got1.Release()
	walked1.waitReached(t, done1)

	walked2.Release()
	waitDone(t, done2, "G2 (Set of u4)")
	if got, err := skiplistmap.StepCheckBucketList(p.m.base, 1000); err != nil {
		t.Fatalf("after G2 linked e once: %v (list %s)", err, j21Hex(got))
	}

	added := s.stopAt("map.makeBucket2.added", isNode(e))
	walked1.Release()
	added.waitReached(t, done1)
	return &j21Splits{p: p, s: s, e: e, added: added, done1: done1}
}

// finish lets G1 go on. G1 may not end, because the list of levels of e is
// linked the same way; the test does not wait for it longer than a while.
func (j *j21Splits) finish(t *testing.T) {
	t.Helper()
	j.added.Release()
	if !waitAtMost(j.done1, 5*time.Second) {
		t.Logf("G1 (Set of the low key) did not end in 5s after the stop was released")
	}
}

// j21Hex formats the last reverses of rs, which a broken list repeats.
func j21Hex(rs []uint64) string {
	s := "["
	if len(rs) > 6 {
		s, rs = "[... ", rs[len(rs)-6:]
	}
	for i, r := range rs {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%#x", r>>56)
	}
	return s + "]<<56"
}

// J21 (3.3.2): the insertion of makeBucket2 into the list of buckets breaks
// the list in the same form as 2.2: it walks the list once, and the insertion
// at the position found reads the prev again on its own without checking the
// order. Here the second insertion is of the same bucket e, which the two
// splits of 3.2.5 both got. After G1's addBucket(e) the list of buckets must
// still be in descending order and reach its tail; it is linked to itself at
// e.
func Test_Repro_J21_SplitsLinkSameBucketTwice(t *testing.T) {
	j := startJ21Splits(t)
	got, err := skiplistmap.StepCheckBucketList(j.p.m.base, 1000)
	if err != nil {
		t.Errorf("after G1's addBucket(e): %v (list %s)", err, j21Hex(got))
	}
	j.finish(t)
}

// J22 (3.3.2): the check of the order in addBucket does not fire. The check
// after the insertion is the negation of the condition that chose the
// position, on the same two reverses, which no one changes once the buckets
// are claimed. In the schedule of Test_Repro_J21_SplitsLinkSameBucketTwice,
// G1's addBucket(e) returns with the list of buckets broken at e, and the
// check ("addBucket.orderBroken", before the log "brokne relation bucket")
// is not reached.
func Test_Repro_J22_AddBucketCheckMissesBrokenList(t *testing.T) {
	j := startJ21Splits(t)
	got, err := skiplistmap.StepCheckBucketList(j.p.m.base, 1000)
	if err == nil {
		t.Fatalf("the schedule did not break the list of buckets: %s", j21Hex(got))
	}
	if n := j.s.count("map.addBucket.orderBroken", j.e); n == 0 {
		t.Errorf("addBucket(e) left the list of buckets broken (%v), and its check of the order did not fire", err)
	}
	j.finish(t)
}
