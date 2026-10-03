package skiplistmap

import (
	"sync"
	"testing"
)

func TestPoolConcurrentInit(t *testing.T) {
	for round := 0; round < 8; round++ {
		p := newPool[StringKey, any]()
		const workers = 16
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				p.Get(0, func(_ MapItem[StringKey, any],

					mu sync.Locker) {
					if mu != nil {
						mu.Unlock()
					}
				})
			}()
		}
		close(start)
		wg.Wait()
		pool := samepleItemPoolFromListHead[StringKey, any](p.itemPool[0].DirectNext())
		if got := pool.items.Len(); got != workers {
			t.Fatalf("round %d: allocated %d items, want %d", round, got, workers)
		}
	}
}
