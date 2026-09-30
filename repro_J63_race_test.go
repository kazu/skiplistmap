//go:build stephook && race

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// The tests in this file show that //go:norace on getWithFn and on
// itemSlice.CopyFrom hides races on the header of the array of a pool. Each
// makes a race between a plain read of the header inside one of the two
// functions and a CompareAndSwapInt of purgeInEmbedded on the length of the
// same pool. The race detector does not see the plain read in a function
// marked //go:norace, so a test here passes when nothing else races, and
// fails when the directive is removed from a copy of the code.

// j63Pool is a map with the embedded pool whose top bucket 3 has been split
// at 0x38, so that downLevels[0] of the bucket (firstDown) uses the item pool
// of the bucket itself. The pool holds d < k2 < last.
type j63Pool struct {
	m    *WrapHMap
	d    string // the key in the first slot of the pool
	x    string // a key not stored, between d and k2
	last string // the key in the last slot of the pool; a lookup of it finds firstDown
}

// newJ63Pool makes the map of j63Pool. The second digits of the reversed hash
// of d and x are 1, of k2 is 2, of last is 3. With at most 3 items per bucket,
// it sets d, k2, k9 and ka (second digits 9 and a): the fourth Set splits the
// bucket at 0x38, leaving d and k2 in the pool, with its capacity cut to 2.
// Set of last then expands the pool and appends last. It then raises the
// limit to 16, so that the Sets of the tests do not split the bucket. Purge of
// last locks firstDown.muPool, and Set of x locks the muPool of the bucket
// that owns the pool, so the two do not wait for each other.
func newJ63Pool(t *testing.T) *j63Pool {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	ones := regionKeys(0x3, 0x1, 2)
	p := &j63Pool{m: newEmbeddedMap(3), d: ones[0], x: ones[1], last: regionKeys(0x3, 0x3, 1)[0]}
	k2 := regionKeys(0x3, 0x2, 1)[0]
	for _, k := range []string{p.d, k2, regionKeys(0x3, 0x9, 1)[0], regionKeys(0x3, 0xa, 1)[0], p.last} {
		if !p.m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)(p.m.base)

	found, base := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](p.m.base, reverseOf(p.last))
	if found == base {
		t.Fatalf("a lookup of last finds the bucket that owns the pool, not firstDown")
	}
	pool, revs := skiplistmap.StepPoolOf[skiplistmap.StringKey, any](p.m.base, reverseOf(p.last))
	if want := []uint64{reverseOf(p.d), reverseOf(k2), reverseOf(p.last)}; len(revs) != 3 || revs[0] != want[0] || revs[1] != want[1] || revs[2] != want[2] {
		t.Fatalf("pool of last holds %x, want %x", revs, want)
	}
	xpool, _ := skiplistmap.StepPoolOf[skiplistmap.StringKey, any](p.m.base, reverseOf(p.x))
	_, xbase := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](p.m.base, reverseOf(p.x))
	if xpool != pool || xbase != base {
		t.Fatalf("x does not belong to the pool of last and its bucket")
	}
	return p
}

// d is deleted with Delete, which only marks its slot. G1 purges last:
// purgeInEmbedded locks firstDown.muPool and stops at purge.beforeInit. G2
// sets x and stops at set.newKeyLock. The test lets G2 run to its end alone
// on the only P: it locks the muPool of the bucket, and getWithFn reads the
// header of the array of the pool with the plain read *sp.ptrItems(), then
// reuses the slot of d (foundFree), which leaves the header as it is. G1 then
// runs on: last is still in the last slot, so purgeInEmbedded lowers the
// length of the pool with CompareAndSwapInt. Nothing orders the read of G2
// before the store of G1. The race detector does not report the two, because
// getWithFn is marked //go:norace.
//
// The race detector keeps only a few of the last accesses to a word, and the
// accesses of G2 after the read and of G1 before the store can push the read
// out, so that the detector misses the race now and then. The test runs the
// same schedule on three maps so that one of them keeps the read.
func Test_J63NoraceHidesGetWithFnRead(t *testing.T) {
	for i := 0; i < 3; i++ {
		j63GetWithFnOnce(t)
	}
}

func j63GetWithFnOnce(t *testing.T) {
	p := newJ63Pool(t)
	if !p.m.base.Delete(skiplistmap.StringKey(p.d)) {
		t.Fatalf("Delete(d) failed")
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.purge.beforeInit", nil)
	done1 := goStep(t, func() { p.m.Delete(p.last) })
	stop1.waitReached(t, done1)
	stop2 := s.stopAt("map.set.newKeyLock", nil)
	done2 := goStep(t, func() { p.m.Set(p.x, &list_head.ListHead{}) })
	stop2.waitReached(t, done2)

	runOn(t, stop1, stop2, true)
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}

// j63HoldAtLenLowered blocks until hold is closed. The test looks for its name
// in the stacks of the goroutines.
func j63HoldAtLenLowered(hold <-chan struct{}) {
	<-hold
}
