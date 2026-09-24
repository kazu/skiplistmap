package skiplistmap_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

func newDefaultMap() *WrapHMap {
	return newWrapHMap(skiplistmap.New())
}

// runTogether starts fn(g) for g in [0, n) at the same time and waits for all.
func runTogether(n int, fn func(g int)) {
	var start, wg sync.WaitGroup
	start.Add(1)
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			start.Wait()
			fn(g)
		}(g)
	}
	start.Done()
	wg.Wait()
}

// Lookups of absent keys run at the same time. A miss records its key in
// Failreverse only while Failreverse is 0, so the test clears it first.
func Test_ConcurrentGetMissing(t *testing.T) {
	params := append(crashMapParams(), crashMapParam{"default", newDefaultMap})
	for _, p := range params {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			prefill(t, m, 1000)
			skiplistmap.Failreverse = 0
			runWithDeadline(t, time.Minute, func() {
				runTogether(8, func(g int) {
					for i := 0; i < 1000; i++ {
						if _, ok := m.Get(fmt.Sprintf("missing-%d-%d", g, i)); ok {
							t.Errorf("Get(missing-%d-%d) found", g, i)
							return
						}
					}
				})
			})
		})
	}
}
