//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

const bucketStateActive = 2

// twoSplits holds two goroutines that split the same bucket of a map at the
// same time, both stopped at makeBucket.claimed.
type twoSplits struct {
	m     *WrapHMap
	s     *stepper
	items []skiplistmap.SampleItem[skiplistmap.StringKey, any]

	extra []skiplistmap.SampleItem[skiplistmap.StringKey, any]

	// the items of the extra keys, not stored
	stored []string

	b                 unsafe.Pointer // the new bucket the winner got
	win, lose         *stepStop      // the winner and the loser of the state CAS
	winDone, loseDone <-chan struct{}
}

// startTwoSplits sets up the two splits of Test_StepSplitLoserKeepsWinnersDummy.
// A first split of the region at a higher position leaves the slot of the
// second split in downLevels unclaimed, so that both goroutines take the
// state CAS path of bucketFromPool for it. The loser stops with the bucket
// of the winner before the fix, and with nil after it.
func startTwoSplits(t *testing.T, extra []string) *twoSplits {
	t.Helper()
	const top = 0x3
	upper := regionKeys(top, 0xc, 8)
	lower := regionKeys(top, 0x0, 16)
	keys := append(append(append([]string{}, upper...), lower...), extra...)
	items := newStepItems(keys)
	upperItems := items[:len(upper)]
	lowerItems := items[len(upper) : len(upper)+len(lower)]
	sp := &twoSplits{items: items, extra: items[len(upper)+len(lower):]}
	sp.m = newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](2)(sp.m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(sp.m.base)
	sp.s = newStepper(t)
	s, m := sp.s, sp.m

	for i := range upperItems {
		m.base.StoreItem(&upperItems[i])
		sp.stored = append(sp.stored, string(upperItems[i].Key()))
		if s.total("map.makeBucket.claimed") > 0 {
			break
		}
	}
	first, _ := s.args("map.makeBucket.claimed")
	if first == nil {
		t.Fatalf("storing %d keys did not split the region", len(sp.stored))
	}
	t.Logf("first split: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](first))

	// The first lower key whose store splits the bucket stops as the winner.
	next := 0
	for ; next < len(lowerItems); next++ {
		st := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&lowerItems[next])))
		d := goStep(t, func(it *skiplistmap.SampleItem[skiplistmap.StringKey, any]) func() {
			return func() { m.base.StoreItem(it) }
		}(&lowerItems[next]))
		sp.stored = append(sp.stored, string(lowerItems[next].Key()))
		select {
		case <-st.reached:
			sp.win, sp.winDone = st, d
		case <-d:
		case <-time.After(10 * time.Second):
			t.Fatalf("StoreItem(%q) neither split nor finished", string(lowerItems[next].Key()))
		}
		if sp.win != nil {
			next++
			break
		}
	}
	if sp.win == nil || next == len(lowerItems) {
		t.Fatalf("no lower key but the last split the bucket")
	}
	sp.b = sp.win.a
	t.Logf("second split: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](sp.b))

	// The next lower key splits the same bucket.
	sp.lose = s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&lowerItems[next])))
	sp.loseDone = goStep(t, func() { m.base.StoreItem(&lowerItems[next]) })
	sp.stored = append(sp.stored, string(lowerItems[next].Key()))
	sp.lose.waitReached(t, sp.loseDone)
	if b2 := sp.lose.a; b2 != nil && b2 != sp.b {
		t.Fatalf("the second store split another bucket: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](b2))
	}
	t.Logf("loser got the bucket of the winner: %v", sp.lose.a != nil)
	return sp
}

// Two goroutines split the same bucket at the same time. The loser of the
// state CAS must not run the onOk of the winner: onOk makes the new bucket
// active, and only the winner knows when the bucket is linked.
//
// Interleaving: the winner G1 and the loser G2 stop at makeBucket.claimed.
// Before the fix both get the new bucket b, and G2 also gets the onOk that G1
// stored in b. G1 runs until the dummy of b is linked and stops at
// insertBucket.dummyLinked, before b is linked into the list of buckets and
// before its level turns positive. G2 then runs to the end: it finds the
// dummy linked and returns ErrBucketAlreadyExit, and its deferred call runs
// the onOk of G1. b becomes active while G1 has not linked it yet.
func Test_ReproA4LoserMakesBucketActiveBeforeLinked(t *testing.T) {
	sp := startTwoSplits(t, nil)
	s, b := sp.s, sp.b

	linked := s.stopAt("map.insertBucket.dummyLinked", isNode(b))
	sp.win.Release()
	linked.waitReached(t, sp.winDone)
	sp.lose.Release()
	waitDone(t, sp.loseDone, "loser")

	if l := skiplistmap.StepBucketLevel(b); l >= 0 {
		t.Fatalf("the level of the new bucket is %d while the winner has not linked it", l)
	}
	if st := skiplistmap.StepBucketState(b); st == bucketStateActive {
		t.Errorf("the new bucket (reverse %016x, level %d) is active before the winner links it", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](b), skiplistmap.StepBucketLevel(b))
	}

	linked.Release()
	waitDone(t, sp.winDone, "winner")
	if st := skiplistmap.StepBucketState(b); st != bucketStateActive {
		t.Errorf("the state of the new bucket is %d after the winner finished, want %d", st, bucketStateActive)
	}
	if n := s.count("map.insertBucket.begin", b); n != 1 {
		t.Errorf("the new bucket was inserted %d times, want 1", n)
	}
	assertStoredInOrder(t, sp.m, sp.stored)
	runtime.KeepAlive(sp.items)
}
