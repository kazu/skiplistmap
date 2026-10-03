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
// entry and reads its key and state, and returns. Nothing orders those reads
// before G1 resumes entryHMap.Delete. Delete therefore retains the immutable
// key and changes only the atomic deleted bit.
func Test_J62EntryDeleteRacesLockFreeGet(t *testing.T) {
	m := skiplistmap.NewHMap[skiplistmap.StringKey, any]()
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](1 << 20)(m)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m)

	keys := adjacentKeys(3)
	for _, k := range keys {
		if !m.StoreItem(skiplistmap.NewEntryMap[skiplistmap.StringKey, any](skiplistmap.StringKey(k), k)) {
			t.Fatalf("StoreItem(%q) failed", k)
		}
	}
	if _, ok := m.Get(skiplistmap.StringKey(keys[1])); !ok {
		t.Fatalf("Get(k1) = false before the test")
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.delete.found", nil)
	done1 := goStep(t, func() { m.Delete(skiplistmap.StringKey(keys[1])) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Get(skiplistmap.StringKey(keys[1])) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
