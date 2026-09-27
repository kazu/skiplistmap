//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// j10BlockedInExpand returns the number of goroutines blocked on a lock whose
// nearest frame of this package is samepleItemPool._expand.
func j10BlockedInExpand() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	cnt := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(g, "Lock(") {
			continue
		}
		for _, line := range strings.Split(g, "\n") {
			if strings.HasPrefix(line, "github.com/kazu/skiplistmap.") {
				if strings.HasPrefix(line, "github.com/kazu/skiplistmap.(*samepleItemPool)._expand(") {
					cnt++
				}
				break
			}
		}
	}
	return cnt
}

// j10WaitBlockedInExpand waits up to 10 seconds until want goroutines are
// blocked in samepleItemPool._expand or done is closed, and returns the
// number of goroutines blocked there.
func j10WaitBlockedInExpand(want int, done <-chan struct{}) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n := j10BlockedInExpand(); n >= want {
			return n
		}
		select {
		case <-done:
			return j10BlockedInExpand()
		case <-time.After(time.Millisecond):
		}
	}
	return j10BlockedInExpand()
}

// The pool holds 63 of its 64 items, as in
// Test_Repro_3_1_6_PoolLastSlotLoserWaitsForItself.
//
// G2 runs Set(x) and stops at pool.lastSlot, after it read the length 63 and
// before it locks the mutex of the pool. The main goroutine runs Set(y) to
// the end: it takes the last item, so the length is 64, and it unlocks the
// mutex when Set returns. G2 resumes, locks the mutex, fails the CAS of the
// length, keeps the mutex since TryLock fails, calls Get again, finds the
// pool full and waits for the mutex it holds in _expand (3.1.6).
//
// G3 and G4 then run Set of two more keys of the same pool. The free list of
// the pool is empty and the pool is full, so each Get goes to _expand and
// waits for the mutex that G2 holds. Neither Set returns: after the loser of
// the last item stops, every Set that has to expand the pool stops too.
func Test_Repro_J10_LaterSetsWaitForStoppedLoser(t *testing.T) {
	keys := adjacentKeys(67)
	m := newStepMap()
	fillPoolButLast(m, keys)

	s := newStepper(t)
	stop := s.stopAt("map.pool.lastSlot", nil)
	done2 := goStep(t, func() { m.base.Set(keys[63], keys[63]) })
	stop.waitReached(t, done2)
	m.base.Set(keys[64], keys[64])
	stop.Release()

	if n := j10WaitBlockedInExpand(1, done2); n >= 1 {
		t.Logf("%d goroutine waits in _expand after the loser of the last item resumed", n)
	}

	done3 := goStep(t, func() { m.base.Set(keys[65], keys[65]) })
	done4 := goStep(t, func() { m.base.Set(keys[66], keys[66]) })
	allDone := make(chan struct{})
	go func() {
		<-done3
		<-done4
		close(allDone)
	}()
	n := j10WaitBlockedInExpand(3, allDone)
	if !waitAtMost(allDone, 5*time.Second) {
		t.Fatalf("Set(%q) and Set(%q) did not finish; %d goroutines wait for the mutex of the pool in _expand",
			keys[65], keys[66], n)
	}
	waitDone(t, done2, "Set of the goroutine that lost the last item")

	assertStoredInOrder(t, m, keys)
}
