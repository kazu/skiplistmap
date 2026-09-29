//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// putBackTraverseMode puts back the traversal mode of lista at the end of the
// test. _expand sets it to WaitNoMark for the whole process, and a failed or
// stopped _expand leaves it there.
func putBackTraverseMode(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { list_head.DefaultModeTraverse.Option(list_head.Direct()) })
}

// exitAfterRelease makes the goroutine that stops at st exit when st is
// released, instead of running on with the pool it got there. The tests below
// stop a Get that took a node that is not a pool; running on from there reads
// and writes memory of other objects. The goroutine holds no lock at the
// points these tests stop it at.
func exitAfterRelease(s *stepper, st *stepStop) {
	var once sync.Once
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		s.at("map."+point, a, b, nil)
		s.mu.Lock()
		stopped := st.used && "map."+point == st.point && st.a == a && st.b == b
		s.mu.Unlock()
		if !stopped {
			return
		}
		exit := false
		once.Do(func() { exit = true })
		if exit {
			runtime.Goexit()
		}
	})
}

// stepLista passes the step points of lista to s, prefixed by "lista.".
func stepLista(t *testing.T, s *stepper) {
	t.Helper()
	list_head.SetStepHook(func(point string, a, b, c *list_head.ListHead) {
		s.at("lista."+point, unsafe.Pointer(a), unsafe.Pointer(b), unsafe.Pointer(c))
	})
	t.Cleanup(func() { list_head.SetStepHook(nil) })
}

// fillFirstPool returns 66 keys that share one pool, and sets the first 64 of
// them, which take the 64 items of the first pool of that pool list.
func fillFirstPool(t *testing.T, m *WrapHMap) []string {
	t.Helper()
	keys := adjacentKeys(skiplistmap.CntOfPersamepleItemPool + 2)
	setKeys(t, m, keys[:skiplistmap.CntOfPersamepleItemPool])
	return keys
}

// G_c sets the 65th key, finds the pool sp full and stops at pool.get.expand,
// before it reads the next of sp. The main goroutine sets the 66th key, which
// also finds sp full and runs _expand to the end: sp is copied into a new
// pool nPool, nPool is linked into the pool list, and sp.Init() links sp to a
// fresh start and end of its own. When G_c resumes, the next of sp is that
// fresh end, whose next is itself, so G_c takes it as "no next pool" and
// calls _expand on sp once more. Before the fix, _expand had no check for a
// pool already expanded: it copied the old array again, and
// RepaireSliceAfterCopy failed at the first outside neighbor, which already
// pointed into the array of nPool; _expand returned EPoolExpandFail and the
// Get of G_c panicked with "already deleted". _expand now sees that sp was
// expanded and does not expand it again.
func Test_Repro_3_1_3_GetExpandsAPoolExpandedMeanwhile(t *testing.T) {
	putBackTraverseMode(t)
	m := newStepMap()
	keys := fillFirstPool(t, m)

	s := newStepper(t)
	sa := s.stopAt("map.pool.get.expand", nil)
	doneC := goStep(t, func() { m.Set(keys[64], &list_head.ListHead{}) })
	sa.waitReached(t, doneC)
	if !m.Set(keys[65], &list_head.ListHead{}) {
		t.Fatalf("Set(%q) failed", keys[65])
	}
	if n := s.count("map.pool.expand.copied", sa.a); n != 1 {
		t.Fatalf("the Set of the 66th key copied the pool %d times, want 1", n)
	}
	sa.Release()
	waitDone(t, doneC, "Set of the 65th key")

	if n := s.count("map.pool.expand.copied", sa.a); n != 1 {
		t.Errorf("the items of the pool were copied %d times, want 1: G_c expanded the pool again", n)
	}
	list_head.DefaultModeTraverse.Option(list_head.Direct())
	assertStoredIfLinked(t, m, keys)
}

// The same as Test_Repro_3_1_3_GetExpandsAPoolExpandedMeanwhile, but G_c has
// already read the next of sp and found no next pool, and stops at
// pool.expand.begin, before it locks sp.mu. The main goroutine sets the 66th
// key and runs _expand of sp to the end. When G_c resumes, it locks sp.mu and
// expands sp again with nothing to tell that sp is already expanded, and
// panics with "already deleted" in the same way.
func Test_Repro_3_1_3_ExpandAfterWaitingForTheLockExpandsAgain(t *testing.T) {
	putBackTraverseMode(t)
	m := newStepMap()
	keys := fillFirstPool(t, m)

	s := newStepper(t)
	sb := s.stopAt("map.pool.expand.begin", nil)
	doneC := goStep(t, func() { m.Set(keys[64], &list_head.ListHead{}) })
	sb.waitReached(t, doneC)
	if !m.Set(keys[65], &list_head.ListHead{}) {
		t.Fatalf("Set(%q) failed", keys[65])
	}
	if n := s.count("map.pool.expand.copied", sb.a); n != 1 {
		t.Fatalf("the Set of the 66th key copied the pool %d times, want 1", n)
	}
	sb.Release()
	waitDone(t, doneC, "Set of the 65th key")

	if n := s.count("map.pool.expand.copied", sb.a); n != 1 {
		t.Errorf("the items of the pool were copied %d times, want 1: G_c expanded the pool again", n)
	}
	list_head.DefaultModeTraverse.Option(list_head.Direct())
	assertStoredIfLinked(t, m, keys)
}

// G_c sets the 65th key, finds the pool sp full and stops at pool.get.expand.
// G_b sets the 66th key, finds sp full, runs _expand, and stops at
// pool.expand.marked: MarkForDelete has marked both links of sp and unlinked
// sp, so sp.next is E|1, the tail E of the pool list with the mark bit. When
// G_c resumes, it reads sp.next with DirectNext, which keeps the mark bit,
// reads the next of E|1 one byte past the next of E, finds it not equal to
// E|1 and takes E|1 as the next pool. The test checks the node at
// pool.get.nextPool and makes G_c exit there: running on, Get reads the
// memory at E|1 minus the offset of the list node as a pool.
func Test_Repro_3_1_3_GetTakesTheMarkedNextAsAPool(t *testing.T) {
	putBackTraverseMode(t)
	m := newStepMap()
	keys := fillFirstPool(t, m)

	s := newStepper(t)
	sa := s.stopAt("map.pool.get.expand", nil)
	sm := s.stopAt("map.pool.expand.marked", nil)
	sn := s.stopAt("map.pool.get.nextPool", nil)
	exitAfterRelease(s, sn)

	doneC := goStep(t, func() { m.Set(keys[64], &list_head.ListHead{}) })
	sa.waitReached(t, doneC)
	doneB := goStep(t, func() { m.Set(keys[65], &list_head.ListHead{}) })
	sm.waitReached(t, doneB)
	sa.Release()

	select {
	case <-sn.reached:
		if uintptr(sn.b)&1 != 0 {
			t.Errorf("Get took the next of the pool %p, %#x, as a next pool: the mark bit is set", sn.a, uintptr(sn.b))
		}
		if !skiplistmap.StepIsLinkedPool(m.base, sn.b) {
			t.Errorf("Get took %#x as a next pool, which is not a pool in the pool list", uintptr(sn.b))
		}
		sn.Release()
	case <-doneC:
	case <-time.After(time.Second):
		// G_c waits for G_b, which is stopped.
	}
	sm.Release()
	waitDone(t, doneB, "Set of the 66th key")
	waitDone(t, doneC, "Set of the 65th key")
}

// G_b sets the 65th key, finds the pool sp full, runs _expand and stops at
// pool.expand.marked: _expand has set the traversal mode of lista to
// WaitNoMark, and MarkForDelete has unlinked sp, so the head H of the pool
// list points to its tail E. G_d sets the 66th key. Pool.Get takes
// H.Next(): E is not marked, so Next returns E, and Pool.Get takes the memory
// before E as a pool. The test checks the node at pool.get.pool and makes G_d
// exit there.
func Test_Repro_3_1_3_GetTakesTheTailOfThePoolListAsAPool(t *testing.T) {
	putBackTraverseMode(t)
	m := newStepMap()
	keys := fillFirstPool(t, m)

	s := newStepper(t)
	sm := s.stopAt("map.pool.expand.marked", nil)
	doneB := goStep(t, func() { m.Set(keys[64], &list_head.ListHead{}) })
	sm.waitReached(t, doneB)

	// Registered only now: every Set above passes pool.get.pool.
	sg := s.stopAt("map.pool.get.pool", nil)
	exitAfterRelease(s, sg)
	doneD := goStep(t, func() { m.Set(keys[65], &list_head.ListHead{}) })
	sg.waitReached(t, doneD)
	if tail := (*list_head.ListHead)(sg.a); tail != nil && tail.DirectNext() == tail {
		t.Errorf("Pool.Get took %p, the tail of the pool list, as a pool", sg.a)
	}
	if !skiplistmap.StepIsLinkedPool(m.base, sg.a) {
		t.Errorf("Pool.Get took %p, which is not a pool in the pool list", sg.a)
	}
	sg.Release()
	waitDone(t, doneD, "Set of the 66th key")
	sm.Release()
	waitDone(t, doneB, "Set of the 65th key")
}

// G_b sets the 65th key, finds the pool sp full, runs _expand and stops in
// MarkForDelete of lista at del.marked: _expand has set the traversal mode to
// WaitNoMark, both links of sp are marked, and the head H of the pool list
// still points to sp. G_d sets the 66th key. Pool.Get takes H.Next(), which
// reads sp, finds it marked 100 times and returns nil, and Pool.Get takes the
// memory below address 0 as a pool; its first read of the pool faults. The
// test checks the node at pool.get.pool and makes G_d exit there.
func Test_Repro_3_1_3_GetTakesNilAsAPoolWhileMarking(t *testing.T) {
	putBackTraverseMode(t)
	m := newStepMap()
	keys := fillFirstPool(t, m)

	s := newStepper(t)
	stepLista(t, s)
	sm := s.stopAt("lista.del.marked", nil)
	doneB := goStep(t, func() { m.Set(keys[64], &list_head.ListHead{}) })
	sm.waitReached(t, doneB)

	// Registered only now: every Set above passes pool.get.pool.
	sg := s.stopAt("map.pool.get.pool", nil)
	exitAfterRelease(s, sg)
	doneD := goStep(t, func() { m.Set(keys[65], &list_head.ListHead{}) })
	sg.waitReached(t, doneD)
	if sg.a == nil {
		t.Errorf("Pool.Get took nil as the list node of a pool")
	}
	if !skiplistmap.StepIsLinkedPool(m.base, sg.a) {
		t.Errorf("Pool.Get took %p, which is not a pool in the pool list", sg.a)
	}
	sg.Release()
	waitDone(t, doneD, "Set of the 66th key")
	sm.Release()
	waitDone(t, doneB, "Set of the 65th key")
}
