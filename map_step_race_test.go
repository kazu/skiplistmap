//go:build stephook && race

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// The tests in this file fail only through the race detector. Each stops two
// goroutines that split the same bucket at chosen points, and then lets both
// run on with nothing ordering them.

func newSplitMap() *WrapHMap {
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](2)(m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m.base)
	return m
}

// runOn lets the goroutines stopped at first and at second run on. It removes
// the hooks first, because the lock in the stepper would order the two. With
// secondAlone, the second one runs to its end on the only P before the first
// one resumes. Otherwise both run on together.
func runOn(t *testing.T, first, second *stepStop, secondAlone bool) {
	elist_head.SetStepHook(nil)
	skiplistmap.SetStepHook(nil)
	if !secondAlone {
		first.Release()
		second.Release()
		return
	}
	procs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(procs) })
	second.Release()
	runtime.Gosched()
	first.Release()
}

// splitTogether stores keys of one region until a store stops at first, which
// is inside the first split of the region. It then stores more keys until a
// store that splits the same bucket stops at second, and lets the two stores
// run on with runOn.
func splitTogether(t *testing.T, first, second string, secondAlone bool) {
	items := newStepItems(regionKeys(0x3, 0xc, 16))
	m := newSplitMap()

	s := newStepper(t)
	stop1 := s.stopAt(first, nil)
	stored, next, done1 := storeUntilStop(t, m, items, stop1)
	stop2 := s.stopAt(second, nil)
	more, n, done2 := storeUntilStop(t, m, items[next:], stop2)
	stored = append(stored, more...)
	next += n

	runOn(t, stop1, stop2, secondAlone)
	waitDone(t, done1, "first split")
	waitDone(t, done2, "second split")
	for ; next < len(items); next++ {
		m.base.StoreItem(&items[next])
		stored = append(stored, string(items[next].Key()))
	}

	assertStoredInOrder(t, m, stored)
	runtime.KeepAlive(items)
}

// The second split claims the slot of downLevels with the CAS on
// cntOfActiveLevels and makes the bucket; the first split then reads
// cntOfActiveLevels and the state of the bucket.
func Test_StepRaceSplitClaimsSlot(t *testing.T) {
	splitTogether(t, "map.bucketFromPool.lenStored", "map.makeBucket.begin", true)
}

// The second split has claimed the new bucket and makes it active while the
// first one waits for it.
func Test_StepRaceSplitActivatesBucket(t *testing.T) {
	splitTogether(t, "map.bucketFromPool.lenStored", "map.makeBucket.claimed", false)
}

// A split has linked the dummy of the new bucket and checks it, while a store
// of the key just before the dummy links its entry before the dummy.
func Test_StepRaceSplitChecksDummy(t *testing.T) {
	items := newStepItems(regionKeys(0x3, 0xc, 16))
	before := newStepItems(regionKeys(0x3, 0x7, 1))
	m := newSplitMap()

	s := newStepper(t)
	stop1 := s.stopAt("map.insertBucket.dummyLinked", nil)
	stored, next, done1 := storeUntilStop(t, m, items, stop1)
	stop2 := s.stopAt("map.add2.found", isNode(nodeOf(&before[0])))
	done2 := goStep(t, func() { m.base.StoreItem(&before[0]) })
	stored = append(stored, string(before[0].Key()))
	stop2.waitReached(t, done2)

	runOn(t, stop1, stop2, true)
	waitDone(t, done1, "split")
	waitDone(t, done2, "store before the dummy")
	for ; next < len(items); next++ {
		m.base.StoreItem(&items[next])
		stored = append(stored, string(items[next].Key()))
	}

	assertStoredInOrder(t, m, stored)
	runtime.KeepAlive(items)
	runtime.KeepAlive(before)
}
