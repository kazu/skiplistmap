//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// Test_ReproA5DummyIsInitializedOnlyOnce shows that, in the current code, the
// dummy of a bucket is not initialized again after it is linked, so that an
// insertion before it never reads it as its own prev and makes a ring.
//
// The only place that initializes a dummy is _InsertBefore, at
// map.insertBucket.begin, called through addBucket by makeBucket for the
// bucket that bucketFromPool returned. A second initialization of the dummy
// of a bucket y needs a second makeBucket that gets y. Two goroutines split
// the same bucket into y: the winner W wins the CAS of the state of y from
// none to init, and the loser L computed y from the same pair of buckets and
// reads the state of y in bucketFromPool. W moves the state to active only in
// onOk, deferred to the end of its makeBucket. So the result of L depends only
// on whether L reads the state before or after the onOk of W. Each subtest
// stops L at makeBucket.pairFound, after it computed y and before
// bucketFromPool, moves W to one of the points of its makeBucket after the
// CAS, and then lets L run bucketFromPool:
//
//   - claimed, insertBucket.begin, insertBucket.dummyLinked, levelFound: the
//     state is init. L gets no bucket and returns (bucketFromPool returns
//     nil, nil when l == level; makeBucket returns on b == nil).
//   - done: the state is active. L gets y, whose dummy is linked, and
//     makeBucket returns before it initializes anything because the head of
//     y is not empty.
//
// In every subtest the dummy of y is initialized once, it is not linked to
// itself, and the lists and the order of the keys are kept.
func Test_ReproA5DummyIsInitializedOnlyOnce(t *testing.T) {
	for _, point := range []string{
		"map.makeBucket.claimed",
		"map.insertBucket.begin",
		"map.insertBucket.dummyLinked",
		"map.makeBucket.levelFound",
		"done",
	} {
		t.Run(point, func(t *testing.T) { runA5LoserAt(t, point) })
	}
}

// runA5LoserAt stops the winner of the split at point ("done" lets it
// finish), runs the bucketFromPool of the loser there, and then lets both
// finish.
func runA5LoserAt(t *testing.T, point string) {
	const top = 0x3
	upper := regionKeys(top, 0xc, 8)
	lower := regionKeys(top, 0x0, 16)
	items := newStepItems(append(append([]string{}, upper...), lower...))
	upperItems, lowerItems := items[:len(upper)], items[len(upper):]
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](2)(m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m.base)

	s := newStepper(t)
	var stored []string
	// A first split of the region at a higher position leaves the slot of y
	// in downLevels unclaimed, so that both goroutines take the state CAS path
	// of bucketFromPool for it.
	for i := range upperItems {
		m.base.StoreItem(&upperItems[i])
		stored = append(stored, string(upperItems[i].Key()))
		if s.total("map.makeBucket.claimed") > 0 {
			break
		}
	}
	if first, _ := s.args("map.makeBucket.claimed"); first == nil {
		t.Fatalf("storing %d keys did not split the region", len(stored))
	}

	// The first lower key whose store splits the bucket stops as the winner.
	var stopW *stepStop
	var doneW <-chan struct{}
	next := 0
	for ; next < len(lowerItems); next++ {
		st := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&lowerItems[next])))
		d := goStep(t, func(it *skiplistmap.SampleItem[skiplistmap.StringKey, any]) func() {
			return func() { m.base.StoreItem(it) }
		}(&lowerItems[next]))
		stored = append(stored, string(lowerItems[next].Key()))
		select {
		case <-st.reached:
			stopW, doneW = st, d
		case <-d:
		case <-time.After(10 * time.Second):
			t.Fatalf("StoreItem(%q) neither split nor finished", string(lowerItems[next].Key()))
		}
		if stopW != nil {
			next++
			break
		}
	}
	if stopW == nil || next >= len(lowerItems) {
		t.Fatalf("no lower key split the bucket before the last one")
	}
	y := stopW.a
	if y == nil {
		t.Fatalf("the winner got no bucket")
	}
	dummy := skiplistmap.StepBucketDummy(y)

	// The loser stores the next lower key and stops after it computed the
	// reverse of the new bucket, before its bucketFromPool. The winner is
	// stopped, so the loser is the only goroutine that reaches pairFound.
	loser := &lowerItems[next]
	stopPair := s.stopAt("map.makeBucket.pairFound", nil)
	stopL := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(loser)))
	doneL := goStep(t, func() { m.base.StoreItem(loser) })
	stored = append(stored, string(loser.Key()))
	stopPair.waitReached(t, doneL)

	// Move the winner to point.
	switch point {
	case "map.makeBucket.claimed":
	case "done":
		stopW.Release()
		waitDone(t, doneW, "winner")
	default:
		st := s.stopAt(point, isNode(y))
		stopW.Release()
		st.waitReached(t, doneW)
		stopW = st
	}
	stateW := skiplistmap.StepBucketState(y)

	// The loser runs its bucketFromPool.
	stopPair.Release()
	stopL.waitReached(t, doneL)
	got := "none"
	if stopL.a != nil {
		got = fmt.Sprintf("%016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](stopL.a))
	}
	t.Logf("winner at %s (y %016x, state %d): the loser got y: %v, got bucket: %v",
		point, skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](y), stateW, stopL.a == y, got)
	if point != "done" && stopL.a != nil {
		// Keep both stopped when the test ends, so that the loser does not
		// initialize the dummy of y again while later tests run.
		stopL.once.Do(func() {})
		stopW.once.Do(func() {})
		t.Fatalf("the loser got a bucket while the winner is at %s", point)
	}
	if point == "done" && stopL.a != y {
		t.Fatalf("the loser did not get y after the winner finished")
	}
	stopL.Release()
	waitDone(t, doneL, "loser")
	if point != "done" {
		stopW.Release()
		waitDone(t, doneW, "winner")
	}

	if n := s.count("map.insertBucket.begin", y); n != 1 {
		t.Fatalf("the dummy of y (%016x) was initialized %d times", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](y), n)
	}
	if skiplistmap.StepDirectNext(dummy) == dummy {
		t.Fatalf("the dummy of y (%016x) points to itself", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](y))
	}
	if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](m.base); err != nil {
		t.Fatalf("lists are broken: %v", err)
	}
	assertStoredInOrder(t, m, stored)
	runtime.KeepAlive(items)
}
