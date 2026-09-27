//go:build stephook && race

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// A map without the embedded pool that never splits a bucket holds entries
// made by NewEntryMap, stored with StoreItem. G1 deletes k1: LoadItem finds
// the entry of k1 and Delete stops at delete.found, before it runs Delete of
// the entry. G2 then gets k1 alone on the only P: Get takes no lock, finds the
// entry and reads its state with the plain read in MapHead.IsIgnored, and
// returns. G1 then runs entryHMap.Delete, which writes nil to the key and the
// deleted bit to the state with plain writes. Nothing orders the reads of G2
// before the writes of G1, so the race detector reports them.
func Test_J62EntryDeleteRacesLockFreeGet(t *testing.T) {
	m := skiplistmap.NewHMap()
	skiplistmap.MaxPefBucket(1 << 20)(m)
	skiplistmap.BucketMode(skiplistmap.CombineSearch4)(m)
	skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptyEntryHMap })(m)
	keys := adjacentKeys(3)
	for _, k := range keys {
		if !m.StoreItem(skiplistmap.NewEntryMap(k, k)) {
			t.Fatalf("StoreItem(%q) failed", k)
		}
	}
	if _, ok := m.Get(keys[1]); !ok {
		t.Fatalf("Get(k1) = false before the test")
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.delete.found", nil)
	done1 := goStep(t, func() { m.Delete(keys[1]) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Get(keys[1]) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
