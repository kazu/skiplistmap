package skiplistmap

import "testing"

// The Locker that Get returns with an item is held in the pool: taking an
// item and telling that it is linked allocates nothing, for an item before
// the last and for the last item, whose Locker also unlocks the pool.
func Test_PoolGetAllocatesNoLocker(t *testing.T) {
	getAndUnlock := func(sp *samepleItemPool) func() {
		return func() {
			_, _, lock := sp.Get()
			lock.Unlock()
		}
	}

	sp := &samepleItemPool{}
	sp.init()
	if allocs := testing.AllocsPerRun(10, getAndUnlock(sp)); allocs != 0 {
		t.Errorf("Get and Unlock of an item allocate %v times, want 0", allocs)
	}

	// AllocsPerRun runs once before it counts: the counted run takes the
	// last item
	sp = &samepleItemPool{}
	sp.init()
	for i := 0; i < CntOfPersamepleItemPool-2; i++ {
		getAndUnlock(sp)()
	}
	if allocs := testing.AllocsPerRun(1, getAndUnlock(sp)); allocs != 0 {
		t.Errorf("Get and Unlock of the last item allocate %v times, want 0", allocs)
	}
	if n := sp.ptrItems().Len(); n != CntOfPersamepleItemPool {
		t.Fatalf("the pool hands out %d items, want %d", n, CntOfPersamepleItemPool)
	}
}
