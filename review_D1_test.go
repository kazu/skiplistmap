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
	var m *skiplistmap.Map[fixedHashKey, any]
	// Search may approach the equal-hash run from either end. Select its
	// insertion order before scheduling the purge, independent of hash seed.
	for order := 0; order < 2; order++ {
		items = make([]hashedItem, 2)
		for i, key := range []string{anchor, "survivor"} {
			items[i].InitEntry(fixedHashKey{key, hash, conflict + uint64(i)}, i)
		}
		m = newHashStepMap()
		m.StoreItem(&items[order])
		m.StoreItem(&items[1-order])
		m.LoadItemByHash(hash, conflict+1)
		candidate, _ := s.args("map.get.found")
		if candidate == nodeOf(&items[0]) {
			break
		}
	}
	defer func() { runtime.KeepAlive(items) }()
	stop := s.stopAt("map.get.found", isNode(nodeOf(&items[0])))
	var found skiplistmap.MapItem[fixedHashKey, any]

	var ok bool
	done := goStep(t, func() { found, ok = m.LoadItemByHash(hash, conflict+1) })
	stop.waitReached(t, done)
	if !m.Purge(fixedHashKey{anchor, hash, conflict}) {
		t.Fatal("purge anchor failed")
	}
	stop.Release()
	waitDone(t, done, "LoadItemByHash")
	if !ok || found.PtrMapHead() != items[1].PtrMapHead() {
		t.Fatalf("lookup lost the continuously live survivor: %v %v", found, ok)
	}
}
