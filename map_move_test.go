package skiplistmap_test

import (
	"sync"
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// growWhile sets new keys from growers goroutines until stop is closed, so
// that the item pools of m grow while the caller writes to present keys. The
// new keys start at the index from.
func growWhile(m *WrapHMap, from int, stop <-chan struct{}) *sync.WaitGroup {
	const growers = 8
	const perGrower = 30000
	var wg sync.WaitGroup
	for g := 0; g < growers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGrower; i++ {
				select {
				case <-stop:
					return
				default:
				}
				m.Set(crashKey(from+g*perGrower+i), &list_head.ListHead{})
			}
		}(g)
	}
	return &wg
}

// A value that Set stores into a present key, while other goroutines set new
// keys and the item pools grow, must be the value that Get returns
// afterwards.
func Test_ConcurrentUpdateWhileGrowing(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const present = 4000
			const writers = 8
			const rounds = 50
			m := p.newMap()
			runWithDeadline(t, time.Minute, func() {
				prefill(t, m, present)
			})
			last := make([]*list_head.ListHead, present)
			runWithDeadline(t, 3*time.Minute, func() {
				stop := make(chan struct{})
				grown := growWhile(m, present, stop)
				var wg sync.WaitGroup
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for r := 0; r < rounds; r++ {
							for k := w; k < present; k += writers {
								v := &list_head.ListHead{}
								if !m.Set(crashKey(k), v) {
									t.Errorf("Set(%q) = false", crashKey(k))
								}
								last[k] = v
							}
						}
					}(w)
				}
				wg.Wait()
				close(stop)
				grown.Wait()
			})
			lost := 0
			for k := range last {
				got, ok := m.Get(crashKey(k))
				if !ok || got != last[k] {
					if lost < 5 {
						t.Errorf("Get(%q) = %p, %v, want the value %p stored last", crashKey(k), got, ok, last[k])
					}
					lost++
				}
			}
			if lost > 0 {
				t.Errorf("%d of %d keys do not hold the value stored last", lost, present)
			}
		})
	}
}

// StoreItem refuses an item that the item pool of a map handed out to Set:
// the pool moves it. It refuses it also after Purge took it out of the list,
// when the item is not linked any more.
func Test_StoreItemRefusesAnItemOfThePool(t *testing.T) {
	m, other := newPoolMap(16), newPoolMap(16)
	if !m.Set("k", &list_head.ListHead{}) {
		t.Fatalf("Set failed")
	}
	item, ok := m.base.LoadItem("k")
	if !ok {
		t.Fatalf("LoadItem did not find the key")
	}
	if other.base.StoreItem(item) {
		t.Errorf("StoreItem of an item of the pool of another map = true")
	}
	if !m.base.Purge("k") {
		t.Fatalf("Purge = false")
	}
	if other.base.StoreItem(item) {
		t.Errorf("StoreItem of an item of the pool of another map, after Purge = true")
	}
	if m.base.StoreItem(item) {
		t.Errorf("StoreItem of an item of the pool of the map itself, after Purge = true")
	}
	if _, ok := other.Get("k"); ok {
		t.Errorf("the other map holds the key")
	}
	if _, ok := m.Get("k"); ok {
		t.Errorf("the map holds the key after Purge")
	}
}

// StoreItem refuses an item still linked also when the key of the item is
// present in the map, where it would store the value of the item into the
// item found: a second StoreItem of the same item, and a StoreItem into
// another map that holds the key.
func Test_StoreItemRefusesALinkedItemOfAKeyPresent(t *testing.T) {
	m, other := newPoolMap(16), newPoolMap(16)
	va, vb := &list_head.ListHead{}, &list_head.ListHead{}
	a := skiplistmap.NewSampleItem[skiplistmap.StringKey, any]("k", va)
	if !m.base.StoreItem(a) {
		t.Fatalf("StoreItem(a) = false")
	}
	if m.base.StoreItem(a) {
		t.Errorf("a second StoreItem of a, which is linked, = true")
	}
	b := skiplistmap.NewSampleItem[skiplistmap.StringKey, any]("k", vb)
	if !other.base.StoreItem(b) {
		t.Fatalf("StoreItem(b) into the other map = false")
	}
	if other.base.StoreItem(a) {
		t.Errorf("StoreItem of a, which is linked in the first map, into the other map = true")
	}
	if got, ok := other.Get("k"); !ok || got != vb {
		t.Errorf("the other map holds %p, %v for k, want the value of b %p", got, ok, vb)
	}
}

// Purge and Set of the same key, again and again in one goroutine, take a new
// item of the pool each time, so the item pools grow while most of their
// items are taken out. Every key must be found after each round.
func Test_PurgeAndSetManyTimes(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const present = 1000
			const rounds = 200
			m := p.newMap()
			prefill(t, m, present)
			for r := 0; r < rounds; r++ {
				for k := 0; k < present; k++ {
					m.Delete(crashKey(k))
					if !m.Set(crashKey(k), &list_head.ListHead{}) {
						t.Fatalf("round %d: Set(%q) = false", r, crashKey(k))
					}
				}
				for k := 0; k < present; k++ {
					if _, ok := m.Get(crashKey(k)); !ok {
						t.Fatalf("round %d: Get(%q) not found", r, crashKey(k))
					}
				}
			}
		})
	}
}

// Purge takes the entry of a key out of the list, so Purge and Set of the
// same few keys, again and again in one goroutine, leave the number of the
// entries of a bucket as it is. The map must not split the bucket for entries
// that are not there, down to a bucket of the hash of a key, which hides the
// key. Every key must be found after its Set.
func Test_PurgeAndSetFewKeysManyTimes(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const present = 16
			const rounds = 4000
			m := p.newMap()
			prefill(t, m, present)
			for r := 0; r < rounds; r++ {
				for k := 0; k < present; k++ {
					m.Delete(crashKey(k))
					if !m.Set(crashKey(k), &list_head.ListHead{}) {
						t.Fatalf("round %d: Set(%q) = false", r, crashKey(k))
					}
					if _, ok := m.Get(crashKey(k)); !ok {
						t.Fatalf("round %d: Get(%q) not found", r, crashKey(k))
					}
				}
			}
		})
	}
}

// A key that Delete deleted, while other goroutines set new keys and the
// item pools grow, must not be found afterwards.
func Test_ConcurrentDeleteWhileGrowing(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			const present = 20000
			const writers = 8
			m := p.newMap()
			runWithDeadline(t, time.Minute, func() {
				prefill(t, m, present)
			})
			runWithDeadline(t, 3*time.Minute, func() {
				stop := make(chan struct{})
				grown := growWhile(m, present, stop)
				var wg sync.WaitGroup
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for k := w; k < present; k += writers {
							if !m.base.Delete(skiplistmap.StringKey(crashKey(k))) {
								t.Errorf("Delete(%q) = false", crashKey(k))
							}
						}
					}(w)
				}
				wg.Wait()
				close(stop)
				grown.Wait()
			})
			found := 0
			for k := 0; k < present; k++ {
				if _, ok := m.Get(crashKey(k)); ok {
					if found < 5 {
						t.Errorf("Get(%q) found a deleted key", crashKey(k))
					}
					found++
				}
			}
			if found > 0 {
				t.Errorf("%d of %d deleted keys are found", found, present)
			}
		})
	}
}
