//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"testing"
	"time"
	"unsafe"
)

// sameTopKeys returns n keys whose reversed hashes share the top 4 bits, so
// that their items come from one pool.
func sameTopKeys(n int) []string {
	groups := map[uint64][]string{}
	for i := 0; ; i++ {
		k := fmt.Sprintf("%d", i)
		h, _ := KeyToHash(k)
		top := bits.Reverse64(h) >> 60
		groups[top] = append(groups[top], k)
		if len(groups[top]) == n {
			return groups[top]
		}
	}
}

// Test_HoldItemFindsThePoolWhileTheOldOneLeavesTheList replays this order:
//
//  1. A map sets 64 keys of one pool, which fill it.
//  2. G1 sets the 65th key: _expand copies the items into a new pool, links
//     it after the old one, repairs the links and stops at
//     pool.expand.repaired. Lookups find the items in the new pool.
//  3. G2 runs holdItem for the item of a key in the new pool: it visits the
//     old pool, the first in the pool list, and stops at holdItem.visit.
//  4. G1 ends: it marks the old pool, unlinks it from the pool list and runs
//     Init on it, so that its next link leads to itself.
//  5. G2 resumes. The new pool holds the item: holdItem must find it, or
//     report that an expand ran so that the caller tries again, and must not
//     report that no pool holds the item.
func Test_HoldItemFindsThePoolWhileTheOldOneLeavesTheList(t *testing.T) {
	keys := sameTopKeys(CntOfPersamepleItemPool + 1)
	h := NewHMap()
	MaxPefBucket(1 << 20)(h)
	for _, k := range keys[:CntOfPersamepleItemPool] {
		h.Set(k, k)
	}
	k0, _ := KeyToHash(keys[0])
	pooler := h.pooler
	old := unsafe.Pointer(samepleItemPoolFromListHead(pooler.itemPool[poolIndex(bits.Reverse64(k0))].Next()))

	repaired, releaseRepaired := make(chan struct{}), make(chan struct{})
	visited, releaseVisit := make(chan struct{}), make(chan struct{})
	var target unsafe.Pointer
	SetStepHook(func(point string, a, b unsafe.Pointer) {
		switch {
		case point == "pool.expand.repaired" && a == old:
			close(repaired)
			<-releaseRepaired
		case point == "holdItem.visit" && a == old && b == target && target != nil:
			close(visited)
			<-releaseVisit
		}
	})
	t.Cleanup(func() { SetStepHook(nil) })

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		h.Set(keys[CntOfPersamepleItemPool], keys[CntOfPersamepleItemPool])
	}()
	select {
	case <-repaired:
	case <-time.After(10 * time.Second):
		t.Fatalf("the expand did not reach pool.expand.repaired")
	}

	item, ok := h.LoadItem(keys[5])
	if !ok {
		t.Fatalf("LoadItem(%q) failed", keys[5])
	}
	target = unsafe.Pointer(item.PtrListHead())
	var sp *samepleItemPool
	var expanding bool
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		sp, expanding = pooler.holdItem(item.PtrMapHead().reverse, item)
	}()
	select {
	case <-visited:
	case <-time.After(10 * time.Second):
		t.Fatalf("holdItem did not visit the old pool")
	}
	close(releaseRepaired)
	<-done1
	close(releaseVisit)
	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		t.Fatalf("holdItem did not return after the old pool left the list")
	}
	if sp != nil {
		sp.linking.Add(-1)
	}
	if sp == nil && !expanding {
		t.Errorf("holdItem reported that no pool holds an item of the new pool")
	}
}
