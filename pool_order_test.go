package skiplistmap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// checkPoolOrder fails the test when a pool of h has a slot within its length
// whose reverse is below the one before it, holes included: the binary search
// of a bucket relies on that order.
func checkPoolOrder(t *testing.T, h *Map[StringKey, int], when string) (pools, holes int) {
	t.Helper()
	var visit func(b *bucket[StringKey, int])
	seen := map[*samepleItemPool[StringKey, int]]bool{}
	visit = func(b *bucket[StringKey, int]) {
		if p := b._itemPool; p != nil && !seen[p] {
			seen[p] = true
			pools++
			items := p.ptrItems()
			var prev uint64
			for i := 0; i < items.Len(); i++ {
				e := items._at(i, false, false)
				if e.IsIgnored() {
					holes++
				}
				if i > 0 && e.reverse < prev {
					t.Errorf("%s: pool %p slot %d reverse %x below slot %d reverse %x (deleted=%v detached=%v linked=%v)", when, p, i, e.reverse, i-1, prev, e.IsDeleted(), e.isDetached(), linkedEntry(&e.ListHead))
					return
				}
				prev = e.reverse
			}
		}
		if downs := b.ptrDownLevels(); downs != nil {
			for d := 0; d < downs.Len(); d++ {
				if c := downs.at(d); c != nil {
					visit(c)
				}
			}
		}
	}
	for i := range h.buckets {
		visit(&h.buckets[i])
	}
	return pools, holes
}

// The slots of an embedded pool stay in the order of their reverse, holes
// included, through inserts in the middle, deletes, purges and the slides
// and rebuilds they cause, in one goroutine and in many.
func TestPoolSlotsStayInReverseOrder(t *testing.T) {
	for _, buckets := range []int{16, 32, 128} {
		t.Run(fmt.Sprintf("sequential/bucket=%d", buckets), func(t *testing.T) {
			h := New[StringKey, int](UseEmbeddedPool[StringKey, int](true), MaxPefBucket[StringKey, int](buckets), BucketMode[StringKey, int](CombineSearch3))
			keys := oneNibbleKeys(300)
			r := rand.New(rand.NewSource(int64(buckets)))
			live := map[string]bool{}
			for step := 0; step < 6000; step++ {
				k := keys[r.Intn(len(keys))]
				switch op := r.Intn(6); {
				case op < 3:
					h.Set(StringKey(k), step)
					live[k] = true
				case op == 3:
					h.Delete(StringKey(k))
					delete(live, k)
				case op == 4:
					h.Purge(StringKey(k))
					delete(live, k)
				default:
					checkPoolOrder(t, h, fmt.Sprintf("step %d", step))
					if t.Failed() {
						t.FailNow()
					}
				}
			}
			pools, holes := checkPoolOrder(t, h, "end")
			if holes == 0 {
				t.Fatalf("the check saw no hole in %d pools; the test needs holes", pools)
			}
		})
		t.Run(fmt.Sprintf("concurrent/bucket=%d", buckets), func(t *testing.T) {
			h := New[StringKey, int](UseEmbeddedPool[StringKey, int](true), MaxPefBucket[StringKey, int](buckets), BucketMode[StringKey, int](CombineSearch3))
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(w)))
					for i := 0; i < 20000; i++ {
						k := StringKey(fmt.Sprintf("k%d", r.Intn(4000)))
						switch r.Intn(5) {
						case 0:
							h.Delete(k)
						case 1:
							h.Purge(k)
						default:
							h.Set(k, i)
						}
					}
				}(w)
			}
			wg.Wait()
			pools, holes := checkPoolOrder(t, h, "end")
			if pools == 0 {
				t.Fatal("no pool")
			}
			t.Logf("pools %d holes %d", pools, holes)
		})
	}
}
