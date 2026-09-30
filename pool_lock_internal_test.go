package skiplistmap

import (
	"sync"
	"testing"
	"time"
)

// A Get that takes the last item of a pool returns the lock of the pool, and
// the caller unlocks it after it linked the item. A Get that expands the pool
// takes its item from the new pool, and must hand on the lock when that item
// is the last one of the new pool: otherwise the lock stays locked, and the
// next expand of the new pool waits for ever.
func Test_PoolGetAfterExpandHandsOnTheLock(t *testing.T) {
	p := newPool[StringKey, any]()
	first := samepleItemPoolFromListHead[StringKey, any](p.itemPool[0].Next())
	// one item per pool, so that the item taken after an expand to two
	// items is the last one
	first._init(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3; i++ {
			p.Get(0, func(item MapItem[StringKey, any],

				mu sync.Locker) {
				if item == nil {
					t.Errorf("Get %d returned no item", i)
				}
				if mu != nil {
					mu.Unlock()
				}
			})
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the third Get did not return: the lock of the pool is not unlocked")
	}
}
