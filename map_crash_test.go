package skiplistmap_test

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// crashMapParam is one Map configuration of the README rows that crash.
type crashMapParam struct {
	name   string
	newMap func() *WrapHMap
}

func newEmbeddedMap(buckets int) *WrapHMap {
	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))
	skiplistmap.MaxPefBucket(buckets)(m.base)
	skiplistmap.BucketMode(skiplistmap.CombineSearch3)(m.base)
	return m
}

func newPoolMap(buckets int) *WrapHMap {
	m := newWrapHMap(skiplistmap.NewHMap())
	skiplistmap.MaxPefBucket(buckets)(m.base)
	skiplistmap.BucketMode(skiplistmap.CombineSearch4)(m.base)
	return m
}

func crashMapParams() []crashMapParam {
	return []crashMapParam{
		{"skiplistmap5 bucket=16", func() *WrapHMap { return newEmbeddedMap(16) }},
		{"skiplistmap5 bucket=32", func() *WrapHMap { return newEmbeddedMap(32) }},
		{"skiplistmap4 bucket=16", func() *WrapHMap { return newPoolMap(16) }},
		{"skiplistmap4 bucket=32", func() *WrapHMap { return newPoolMap(32) }},
	}
}

const crashMapCnt = 100000

func crashKey(i int) string {
	return fmt.Sprintf("%d", i)
}

// runWithDeadline fails the test if fn does not return within d.
// Map.find loops forever on a broken link ring; go test -timeout does not stop it.
func runWithDeadline(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("did not finish within %v\n%s", d, buf[:n])
	}
}

// aggressiveGC makes the collector run as often as possible for the test.
func aggressiveGC(t *testing.T) {
	t.Helper()
	old := debug.SetGCPercent(1)
	t.Cleanup(func() { debug.SetGCPercent(old) })
}

func prefill(t *testing.T, m *WrapHMap, cnt int) {
	t.Helper()
	for i := 0; i < cnt; i++ {
		if !m.Set(crashKey(i), &list_head.ListHead{}) {
			t.Errorf("Set(%q) returned false", crashKey(i))
			return
		}
	}
}

func assertAllFound(t *testing.T, m *WrapHMap, cnt int) {
	t.Helper()
	missing := 0
	for i := 0; i < cnt; i++ {
		if _, ok := m.Get(crashKey(i)); !ok {
			missing++
			if missing <= 5 {
				t.Errorf("Get(%q) not found", crashKey(i))
			}
		}
	}
	if missing > 0 {
		t.Errorf("%d of %d keys not found", missing, cnt)
	}
}

// Item 1: sequential Set of 100k keys must not fault.
func Test_SetSequential(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			aggressiveGC(t)
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				prefill(t, m, crashMapCnt)
			})
			if got := m.base.Len(); got != crashMapCnt {
				t.Errorf("Len() = %d, want %d", got, crashMapCnt)
			}
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, crashMapCnt)
			})
		})
	}
}

// Item 1: Set with runtime.GC() between inserts must not free live elements.
func Test_SetWithForcedGC(t *testing.T) {
	params := append(crashMapParams(),
		crashMapParam{"default", func() *WrapHMap { return newWrapHMap(skiplistmap.New()) }})
	for _, p := range params {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				for i := 0; i < crashMapCnt; i++ {
					m.Set(crashKey(i), &list_head.ListHead{})
					if i%97 == 0 {
						runtime.GC()
					}
				}
			})
			runtime.GC()
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, crashMapCnt)
			})
		})
	}
}

// Item 3: concurrent reads and updates of existing keys (16 goroutines, 50%/50%).
func Test_ConcurrentReadUpdate(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			aggressiveGC(t)
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				prefill(t, m, crashMapCnt)
			})
			const goroutines = 16
			const opsPerGoroutine = 20000
			runWithDeadline(t, 3*time.Minute, func() {
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(g int) {
						defer wg.Done()
						idx := g * 7919
						for i := 0; i < opsPerGoroutine; i++ {
							key := crashKey(idx % crashMapCnt)
							if g%2 == 0 {
								m.Set(key, &list_head.ListHead{})
							} else if _, ok := m.Get(key); !ok {
								t.Errorf("Get(%q) not found during update", key)
								return
							}
							idx++
						}
					}(g)
				}
				wg.Wait()
			})
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, crashMapCnt)
			})
		})
	}
}

// Item 4: Range must visit every inserted key exactly once.
func Test_RangeVisitsAll(t *testing.T) {
	for _, cnt := range []int{1000, crashMapCnt} {
		for _, p := range crashMapParams() {
			t.Run(fmt.Sprintf("%s n=%d", p.name, cnt), func(t *testing.T) {
				m := p.newMap()
				runWithDeadline(t, 2*time.Minute, func() {
					prefill(t, m, cnt)
				})
				seen := map[string]int{}
				runWithDeadline(t, 2*time.Minute, func() {
					m.base.Range(func(k, v interface{}) bool {
						seen[k.(string)]++
						return true
					})
				})
				if len(seen) != cnt {
					t.Errorf("range visited %d distinct keys, want %d", len(seen), cnt)
				}
				for k, n := range seen {
					if n != 1 {
						t.Errorf("key %q visited %d times", k, n)
					}
				}
			})
		}
	}
}

// Item 3: concurrent purge followed by re-insert of the same key (the
// delete-reinsert row of the task 003 harness, 100 goroutines) must neither
// unlock an unlocked bucket mutex nor fault, and every key must remain.
func Test_ConcurrentDeleteReinsert(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				prefill(t, m, crashMapCnt)
			})
			const goroutines = 100
			const opsPerGoroutine = 2000
			runWithDeadline(t, 3*time.Minute, func() {
				var wg sync.WaitGroup
				for g := 0; g < goroutines; g++ {
					wg.Add(1)
					go func(g int) {
						defer wg.Done()
						idx := g * 7919
						for i := 0; i < opsPerGoroutine; i++ {
							key := crashKey(idx % crashMapCnt)
							m.Delete(key)
							m.Set(key, &list_head.ListHead{})
							idx++
						}
					}(g)
				}
				wg.Wait()
			})
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, crashMapCnt)
			})
		})
	}
}

// Item 3/4: a purged key must be re-insertable and found again.
func Test_DeleteReinsert(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			runWithDeadline(t, 2*time.Minute, func() {
				prefill(t, m, crashMapCnt)
			})
			runWithDeadline(t, 2*time.Minute, func() {
				for i := 0; i < crashMapCnt; i += 37 {
					key := crashKey(i)
					if !m.Delete(key) {
						t.Errorf("Delete(%q) returned false", key)
						return
					}
					if _, ok := m.Get(key); ok {
						t.Errorf("Get(%q) found after delete", key)
						return
					}
				}
				for i := 0; i < crashMapCnt; i += 37 {
					key := crashKey(i)
					if !m.Set(key, &list_head.ListHead{}) {
						t.Errorf("Set(%q) after delete returned false", key)
						return
					}
					if _, ok := m.Get(key); !ok {
						t.Errorf("Get(%q) not found after re-insert", key)
						return
					}
				}
			})
			runWithDeadline(t, 2*time.Minute, func() {
				assertAllFound(t, m, crashMapCnt)
			})
		})
	}
}
