//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// newJ56Map returns a map without the embedded pool for entries made by
// NewEntryMap, with the bucket mode mode and at most max items per bucket.
func newJ56Map(mode skiplistmap.SearchMode, max int) *skiplistmap.Map {
	m := skiplistmap.NewHMap()
	skiplistmap.MaxPefBucket(max)(m)
	skiplistmap.BucketMode(mode)(m)
	skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptyEntryHMap })(m)
	return m
}

// Delete now preserves the key, so reusing an entry cannot retarget key 0.
// The original claim: Delete(k) runs entryHMap.Delete on the entry of k, which sets
// its key to nil, and KeyToHash(nil) is (0, 0), the hash pair of the key 0.
// If the caller then stores the same entry again with StoreItem, its lookup
// looks for (0, 0), finds the entry of the key 0 and stores the value of the
// entry of k into it.
//
// This does not happen, because no lookup finds an entry of the key 0. Its
// reversed hash is 0, the reverse of the dummy of the bucket 0. StoreItem
// links such an entry after that dummy (the position of add2 is the first
// entry whose reverse is larger), while _searchBybucket, for a reverse that
// is not above the reverse of the bucket, walks backward from the dummy of
// the bucket and never reaches the entries after it. The test stores the key
// 0 and k in each bucket mode, checks that LoadItem and Get miss the key 0,
// deletes k, stores the entry of k again, and checks that the entry of the
// key 0 still holds its own value.
func Test_J56StoreAfterEntryDeleteDoesNotOverwriteKeyZero(t *testing.T) {
	modes := []skiplistmap.SearchMode{
		skiplistmap.LenearSearchForBucket, skiplistmap.NestedSearchForBucket,
		skiplistmap.CombineSearch, skiplistmap.CombineSearch2, skiplistmap.CombineSearch3,
		skiplistmap.CombineSearch4, skiplistmap.NoItemSearchForBucket, skiplistmap.FalsesSearchForBucket,
	}
	for _, mode := range modes {
		for _, max := range []int{16, 1 << 20} {
			m := newJ56Map(mode, max)
			const k = "k"
			zero := skiplistmap.NewEntryMap(0, "zero")
			e := skiplistmap.NewEntryMap(k, "v")
			for _, it := range []skiplistmap.MapItem{zero, e} {
				if !m.StoreItem(it) {
					t.Fatalf("mode %d max %d: StoreItem(%v) failed", mode, max, it.Key())
				}
			}
			if _, ok := m.LoadItem(0); ok {
				t.Fatalf("mode %d max %d: LoadItem(0) finds the key 0; the reason above does not hold", mode, max)
			}
			if _, ok := m.Get(0); ok {
				t.Fatalf("mode %d max %d: Get(0) finds the key 0; the reason above does not hold", mode, max)
			}
			if !m.Delete(k) {
				t.Fatalf("mode %d max %d: Delete(k) = false", mode, max)
			}
			if e.Key() != k {
				t.Fatalf("mode %d max %d: the key of the entry of k changed to %v after Delete(k), want %v", mode, max, e.Key(), k)
			}
			stored := m.StoreItem(e)
			if v := zero.Value(); v != "zero" {
				t.Errorf("mode %d max %d: the entry of the key 0 holds %v after StoreItem of the deleted entry of k (returned %v)", mode, max, v, stored)
			}
			t.Logf("mode %d max %d: StoreItem of the deleted entry = %v, the entry of the key 0 holds %v, lists: %v",
				mode, max, stored, zero.Value(), skiplistmap.StepCheckLists(m))
		}
	}
}
