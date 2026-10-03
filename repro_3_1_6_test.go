//go:build stephook

package skiplistmap_test

import "github.com/kazu/skiplistmap"

import (
	"runtime"
	"strings"
	"testing"
	"time"

	list_head "github.com/kazu/lista_encabezado"
)

// fillPoolButLast stores keys[:63] with Set, so that the pool that the keys
// share holds 63 of its 64 items and the next Set takes its last item. The
// keys of adjacentKeys share the top 4 bits of their reversed hashes, which
// choose the pool.
func fillPoolButLast(m *WrapHMap, keys []string) {
	for _, k := range keys[:63] {
		m.base.Set(skiplistmap.StringKey(k), &list_head.ListHead{})
	}
}

// waitBlockedInPoolGet waits until a goroutine is blocked on a lock whose
// nearest frame of this package is samepleItemPool.Get itself, not a function
// that Get called.
func waitBlockedInPoolGet(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if !strings.Contains(g, "Lock(") {
				continue
			}
			for _, line := range strings.Split(g, "\n") {
				if strings.HasPrefix(line, "github.com/kazu/skiplistmap.") {
					if strings.HasPrefix(line, "github.com/kazu/skiplistmap.(*samepleItemPool).Get(") {
						return
					}
					break
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine blocked on a lock in samepleItemPool.Get")
}

// The pool holds 63 of its 64 items. G2 runs Set(x): Get reads the length
// 63, sees that it takes the last item, and stops at pool.lastSlot before it
// locks the mutex of the pool. The main goroutine runs Set(y) to the end: it
// reads the length 63 too, locks the mutex, raises the length to 64 and
// unlocks the mutex when Set returns. When G2 resumes, it locks the mutex;
// the check after the lock compares only i+1 with the capacity, so it passes,
// and the CAS of the length from 63 fails. TryLock fails, since G2 holds the
// mutex itself, and G2 calls Get again with the mutex held. The pool is full
// and has no next pool, so Get calls _expand, which locks the same mutex:
// G2 waits for itself and Set(x) never returns.
func Test_Repro_3_1_6_PoolLastSlotLoserWaitsForItself(t *testing.T) {
	keys := adjacentKeys(65)
	m := newStepMap()
	fillPoolButLast(m, keys)

	s := newStepper(t)
	stop := s.stopAt("map.pool.lastSlot", nil)
	done := goStep(t, func() { m.base.Set(skiplistmap.StringKey(keys[63]), &list_head.ListHead{}) })
	stop.waitReached(t, done)
	m.base.Set(skiplistmap.StringKey(keys[64]), &list_head.ListHead{})
	stop.Release()
	waitDone(t, done, "Set of the goroutine that lost the last item")

	assertStoredInOrder(t, m, keys)
}

// The same race in the order of the report. The pool holds 63 of its 64
// items. G2 runs Set(x) and stops at pool.lastSlot. G1 runs Set(y): it takes
// the last item, so it holds the mutex of the pool and the length is 64, and
// it stops at add2.found. G2 resumes and blocks on the mutex in Get. When G1
// resumes, it links y and unlocks the mutex when Set returns. G2 then gets
// the mutex, passes the check that compares only i+1 with the capacity,
// fails the CAS of the length, keeps the mutex since TryLock fails, and
// waits for itself in _expand. Set(x) never returns.
func Test_Repro_3_1_6_PoolLastSlotLoserWaitsForItselfAfterWaiting(t *testing.T) {
	keys := adjacentKeys(65)
	m := newStepMap()
	fillPoolButLast(m, keys)

	s := newStepper(t)
	last := s.stopAt("map.pool.lastSlot", nil)
	found := s.stopAt("map.add2.found", nil)
	done2 := goStep(t, func() { m.base.Set(skiplistmap.StringKey(keys[63]), &list_head.ListHead{}) })
	last.waitReached(t, done2)
	done1 := goStep(t, func() { m.base.Set(skiplistmap.StringKey(keys[64]), &list_head.ListHead{}) })
	found.waitReached(t, done1)
	last.Release()
	waitBlockedInPoolGet(t)
	found.Release()
	waitDone(t, done1, "Set of the goroutine that took the last item")
	waitDone(t, done2, "Set of the goroutine that lost the last item")

	assertStoredInOrder(t, m, keys)
}
