//go:build stephook && race

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// The tests in this file take the races that Test_ConcurrentNewKeys reports
// with -race for a map with the embedded pool, one at a time. Each fails only
// through the race detector.

// n2Map returns a map with the embedded pool that does not split its buckets,
// and n keys of the top bucket 3 in the order of their reversed hashes. The
// keys at the indexes in stored are set.
func n2Map(t *testing.T, n int, stored ...int) (*WrapHMap, []string) {
	t.Helper()
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, n)
	for _, i := range stored {
		if !m.Set(keys[i], &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", keys[i])
		}
	}
	return m, keys
}

// runAloneThenRelease removes the hooks and, on the only P, runs fn in a new
// goroutine to its end before st is released. The test does not wait for fn
// before the release, so nothing orders what fn does before what the
// goroutine stopped at st does after it.
func runAloneThenRelease(t *testing.T, st *stepStop, fn func()) <-chan struct{} {
	t.Helper()
	elist_head.SetStepHook(nil)
	skiplistmap.SetStepHook(nil)
	procs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(procs) })
	done := goStep(t, fn)
	runtime.Gosched()
	st.Release()
	return done
}

// The pool of the top bucket 3 holds k0 < k1 < k2. G1 sets k3, which is
// larger than every key in the pool: appendLast raises the length of the pool
// over its last slot and stops at appendLast.claimed, before it clears the
// state of the slot. G2 then gets k4, a larger key that is not stored, alone
// on the only P: sort.Search in bsearchBybucket takes the length that G1
// raised, looks at the last slot and reads its state with the plain read in
// MapHead.IsDeleted, and G2 returns without taking a lock. G1 then stores the
// state of the slot with atomic.StoreUint32. Nothing orders the read of G2
// before the store of G1, so the race detector reports the two.
func Test_N2AppendLastStateRace(t *testing.T) {
	m, keys := n2Map(t, 5, 0, 1, 2)

	s := newStepper(t)
	stop1 := s.stopAt("map.appendLast.claimed", nil)
	done1 := goStep(t, func() { m.Set(keys[3], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Get(keys[4]) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}

// The pool of the top bucket 3 holds k0 < k1 < k3 < k4 < k5. G1 sets k2:
// insertToPool builds a new array with k2 in it, links it into the list and
// stops at insertToPool.publish, before CopyFrom stores the array into the
// pool. G2 then gets k4 alone on the only P: bsearchBybucket finds its index
// in the old array, and itemSlice.reverseAt reads the array of the pool with a
// plain read. G1 then replaces the array with CompareAndSwapPointer in
// CopyFrom. Nothing orders the read of G2 before the swap of G1, so the race
// detector reports the two.
func Test_N2InsertToPoolArrayRace(t *testing.T) {
	m, keys := n2Map(t, 6, 0, 1, 3, 4, 5)

	s := newStepper(t)
	stop1 := s.stopAt("map.insertToPool.publish", nil)
	done1 := goStep(t, func() { m.Set(keys[2], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Get(keys[4]) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}

// A map with the embedded pool and at most 3 items per bucket holds three keys
// of the top bucket 3, whose second digits of the reversed hash are 1, 2 and
// 9. G1 sets a fourth key of the bucket, whose second digit is a, and stops at
// add2.found. The test releases G1 with the hooks removed, and G1 runs to its
// end alone on the only P: it splits the bucket at 0x38, and makeBucket2
// calls Empty of lista (in bucketFromPoolEmbedded and insertOnLevel),
// which reads list_head.MODE_CONCURRENT. The test then
// makes another map with NewHMap, which writes true to MODE_CONCURRENT with a
// plain write. The test does not wait for G1 before NewHMap, so nothing
// orders the read of G1 before the write, and the race detector reports the
// two. Any goroutine that splits a bucket while another map is made does the
// same, as a subtest left running by a timeout does with the next subtest.
func Test_N2NewHMapModeConcurrentRace(t *testing.T) {
	m := newEmbeddedMap(3)
	var keys []string
	for _, second := range []uint64{0x1, 0x2, 0x9, 0xa} {
		keys = append(keys, regionKeys(0x3, second, 1)[0])
	}
	for _, k := range keys[:3] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.add2.found", nil)
	done1 := goStep(t, func() { m.Set(keys[3], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	elist_head.SetStepHook(nil)
	skiplistmap.SetStepHook(nil)
	procs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(procs) })
	stop1.Release()
	runtime.Gosched()
	skiplistmap.NewHMap[skiplistmap.StringKey, any]()
	waitDone(t, done1, "G1")
}
