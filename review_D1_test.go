//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// The hash-only lookup must not lose a continuously live collision when its
// initial search candidate is removed before the collision walk starts.
func Test_D1HashLookupAfterCandidatePurged(t *testing.T) {
	const anchor = "anchor"
	hash, conflict := skiplistmap.KeyToHash(anchor)
	s := newStepper(t)
	var items []hashedItem
	var m *WrapHMap
	// Search may approach the equal-hash run from either end. Select its
	// insertion order before scheduling the purge, independent of hash seed.
	for order := 0; order < 2; order++ {
		items = make([]hashedItem, 2)
		for i, key := range []string{anchor, "survivor"} {
			items[i].K = key
			items[i].k, items[i].conflict = hash, conflict+uint64(i)
			items[i].SetValue(i)
		}
		m = newStepMap()
		m.base.StoreItem(&items[order])
		m.base.StoreItem(&items[1-order])
		m.base.LoadItemByHash(hash, conflict+1)
		candidate, _ := s.args("map.get.found")
		if candidate == nodeOf(&items[0].SampleItem) {
			break
		}
	}
	defer func() { runtime.KeepAlive(items) }()
	stop := s.stopAt("map.get.found", isNode(nodeOf(&items[0].SampleItem)))
	var found skiplistmap.MapItem
	var ok bool
	done := goStep(t, func() { found, ok = m.base.LoadItemByHash(hash, conflict+1) })
	stop.waitReached(t, done)
	if !m.base.Purge(anchor) {
		t.Fatal("purge anchor failed")
	}
	stop.Release()
	waitDone(t, done, "LoadItemByHash")
	if !ok || found.PtrMapHead() != items[1].PtrMapHead() {
		t.Fatalf("lookup lost the continuously live survivor: %v %v", found, ok)
	}
}
