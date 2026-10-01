//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// keepStopped keeps the goroutine stopped at st stopped when the test ends:
// Release, which the cleanup of the stepper calls, does nothing afterwards.
func keepStopped(st *stepStop) {
	st.once.Do(func() {})
}

// splitSameBucketTwice stores items one by one, each in a new goroutine,
// until one of them stops at makeBucket.claimed as the winner of a split, and
// then stores the next item, which splits the same bucket, and stops it at
// makeBucket.claimed as the loser. It returns both stops, the channels closed
// when the two stores finish, and the keys stored.
func splitSameBucketTwice(t *testing.T, s *stepper, m *WrapHMap, items []skiplistmap.SampleItem[skiplistmap.StringKey, any]) (win, lose *stepStop, winDone, loseDone <-chan struct{}, stored []string) {
	t.Helper()
	next := 0
	for ; next < len(items); next++ {
		st := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&items[next])))
		d := goStep(t, func(it *skiplistmap.SampleItem[skiplistmap.StringKey, any]) func() {
			return func() { m.base.StoreItem(it) }
		}(&items[next]))
		stored = append(stored, string(items[next].Key()))
		select {
		case <-st.reached:
			win, winDone = st, d
		case <-d:
		case <-time.After(10 * time.Second):
			t.Fatalf("StoreItem(%q) neither split nor finished", string(items[next].Key()))
		}
		if win != nil {
			next++
			break
		}
	}
	if win == nil || next == len(items) {
		t.Fatalf("no key but the last split the bucket")
	}
	lose = s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&items[next])))
	loseDone = goStep(t, func() { m.base.StoreItem(&items[next]) })
	stored = append(stored, string(items[next].Key()))
	lose.waitReached(t, loseDone)
	if lose.a != nil && lose.a != win.a {
		t.Fatalf("the second store split another bucket: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](lose.a))
	}
	return
}

// Two splits of two buckets are each made by two goroutines at the same time.
// Before the fix the loser of each split initializes the dummy that the
// winner linked, and the dummy points to itself while its neighbours still
// point to it. An insert before such a dummy links the new entry and the
// dummy to each other only.
//
// Buckets: 0x38.. from a first split, y (0x34..), b' (0x32..) and c (0x33..).
// No key lies in [0x32.., 0x33..).
//
// Interleaving: the winner Gy1 and the loser Gy2 of the split into y stop at
// makeBucket.claimed with y. Gy2 runs to insertBucket.begin, before it
// initializes the dummy Y of y. Gy1 runs to the end: Y and y are linked.
// Keys stored one by one below y split the bucket below y into b'. Keys in
// [0x33.., 0x34..) make the winner Gc1 and the loser Gc2 of the split into c
// stop at makeBucket.claimed with c. Gc2 runs to insertBucket.begin, Gc1 runs
// to insertBucket.dummyLinked: the dummy C of c is linked and c is not in the
// list of buckets. Gc2 initializes C, which now points to itself, fails to
// link it again and stops at insertBucket.dummyLinked. G3 stores e, a key
// above the keys in [0x33.., 0x34..): the search from the dummy B' of b'
// stops at C, so add2 takes the entry of the bucket above b', Y, as the
// position, checks the order against the entry before Y, and stops at
// elist insert.begin of e before Y. Gy2 then initializes Y, which now points
// to itself, fails to link it again and stops at insertBucket.dummyLinked.
// G3 resumes: it reads Y as the prev of Y and links e between Y and Y. Y and
// e point to each other only, and the keys between them are lost from the
// list.
func Test_ReproA5InsertBeforeInitializedDummyMakesARing(t *testing.T) {
	prevs := elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(prevs...) })

	bKeys := regionKeys(0x3, 0x1, 32)
	cKeys := regionKeys(0x3, 0x3, 16)
	sp := startTwoSplits(t, append(append([]string{}, bKeys...), cKeys...))
	s, m, y := sp.s, sp.m, sp.b
	bItems, cItems := sp.extra[:len(bKeys)], sp.extra[len(bKeys):]
	eItem := &cItems[len(cItems)-1]
	cItems = cItems[:len(cItems)-1]
	if r := skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](y) >> 56; r != 0x34 {
		t.Fatalf("the new bucket has reverse %02x.., want 34..", r)
	}
	dummyY := skiplistmap.StepBucketDummy(y)
	fixed := sp.lose.a == nil

	// Gy2 stops before it initializes Y, and Gy1 links Y and y.
	var yBegin *stepStop
	if fixed {
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser of y")
	} else {
		yBegin = s.stopAt("map.insertBucket.begin", isNode(y))
		sp.lose.Release()
		yBegin.waitReached(t, sp.loseDone)
	}
	sp.win.Release()
	waitDone(t, sp.winDone, "winner of y")

	// The bucket below y splits into b'.
	var bp unsafe.Pointer
	for i := range bItems {
		m.base.StoreItem(&bItems[i])
		sp.stored = append(sp.stored, string(bItems[i].Key()))
		if a, _ := s.args("map.makeBucket.claimed"); a != nil &&
			skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](a)>>56 == 0x32 {
			bp = a
			break
		}
	}
	if bp == nil {
		t.Fatalf("storing %d keys below y did not split the bucket below y", len(bItems))
	}

	// Gc1 and Gc2 split b' into c.
	cWin, cLose, cWinDone, cLoseDone, cStored := splitSameBucketTwice(t, s, m, cItems)
	sp.stored = append(sp.stored, cStored...)
	c := cWin.a
	if r := skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](c) >> 56; r != 0x33 {
		t.Fatalf("the split above b' made reverse %02x.., want 33..", r)
	}
	if fixed != (cLose.a == nil) {
		t.Fatalf("the loser of c got %p, the loser of y got %p", cLose.a, sp.lose.a)
	}

	if fixed {
		cLose.Release()
		waitDone(t, cLoseDone, "loser of c")
		cWin.Release()
		waitDone(t, cWinDone, "winner of c")
		m.base.StoreItem(eItem)
		sp.stored = append(sp.stored, string(eItem.Key()))
		if n := s.count("map.insertBucket.begin", y); n != 1 {
			t.Errorf("y was inserted %d times, want 1", n)
		}
		if n := s.count("map.insertBucket.begin", c); n != 1 {
			t.Errorf("c was inserted %d times, want 1", n)
		}
		if next := skiplistmap.StepDirectNext(nodeOf(eItem)); next == dummyY && skiplistmap.StepDirectNext(dummyY) == nodeOf(eItem) {
			t.Errorf("e and the dummy of y point to each other only")
		}
		assertStoredInOrder(t, m, sp.stored)
		runtime.KeepAlive(sp.items)
		return
	}

	// Gc2 stops before it initializes C, and Gc1 links C.
	dummyC := skiplistmap.StepBucketDummy(c)
	cBegin := s.stopAt("map.insertBucket.begin", isNode(c))
	cLose.Release()
	cBegin.waitReached(t, cLoseDone)
	cLinked := s.stopAt("map.insertBucket.dummyLinked", isNode(c))
	cWin.Release()
	cLinked.waitReached(t, cWinDone)
	// Gc2 initializes C and fails to link it again.
	cLinked2 := s.stopAt("map.insertBucket.dummyLinked", isNode(c))
	cBegin.Release()
	cLinked2.waitReached(t, cLoseDone)
	if skiplistmap.StepDirectNext(dummyC) != dummyC {
		t.Fatalf("the dummy of c does not point to itself after the loser initialized it")
	}

	// G3 takes Y as the position of e and stops before it links e.
	e := nodeOf(eItem)
	eBegin := s.stopAt("elist.insert.begin", func(a, _, c unsafe.Pointer) bool { return a == e && c == dummyY })
	eDone := goStep(t, func() { m.base.StoreItem(eItem) })
	eBegin.waitReached(t, eDone)

	// Gy2 initializes Y and fails to link it again.
	yLinked := s.stopAt("map.insertBucket.dummyLinked", isNode(y))
	yBegin.Release()
	yLinked.waitReached(t, sp.loseDone)
	if skiplistmap.StepDirectNext(dummyY) != dummyY {
		t.Fatalf("the dummy of y does not point to itself after the loser initialized it")
	}

	// G3 links e. It then splits the bucket of e, and stops there: the walk
	// over the entries of the new bucket would go round the ring.
	eClaimed := s.stopAt("map.makeBucket.claimed", isSecond(e))
	eBegin.Release()
	select {
	case <-eClaimed.reached:
		t.Logf("StoreItem(e) stopped at the split of the bucket of e")
	case <-eDone:
		t.Logf("StoreItem(e) finished")
	case <-time.After(10 * time.Second):
		t.Fatalf("StoreItem(e) neither split nor finished after it linked e")
	}
	if skiplistmap.StepDirectNext(dummyY) == e && skiplistmap.StepDirectNext(e) == dummyY {
		t.Errorf("e (reverse %016x) and the dummy of y point to each other only",
			reverseOf(string(eItem.Key())))
	}
	for _, st := range []*stepStop{cLinked, cLinked2, yLinked, eClaimed} {
		keepStopped(st)
	}
	runtime.KeepAlive(sp.items)
}
