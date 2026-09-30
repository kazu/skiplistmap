package skiplistmap

import (
	"sync"
	"testing"
)

func TestPoolConcurrentInit(t *testing.T) {
	for round := 0; round < 8; round++ {
		p := newPool()
		const workers = 16
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				p.Get(0, func(_ MapItem, mu sync.Locker) {
					if mu != nil {
						mu.Unlock()
					}
				})
			}()
		}
		close(start)
		wg.Wait()
		pool := samepleItemPoolFromListHead(p.itemPool[0].DirectNext())
		if got := len(pool.items); got != workers {
			t.Fatalf("round %d: allocated %d items, want %d", round, got, workers)
		}
	}
}
