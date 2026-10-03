//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// newJ56Map returns a map without the embedded pool for entries made by
// NewEntryMap, with the bucket mode mode and at most max items per bucket.
func newJ56Map(mode skiplistmap.SearchMode, max int) *skiplistmap.Map[skiplistmap.Uint64Key, any] {
	m := skiplistmap.NewHMap[skiplistmap.Uint64Key, any]()
	skiplistmap.MaxPefBucket[skiplistmap.Uint64Key, any](max)(m)
	skiplistmap.BucketMode[skiplistmap.Uint64Key, any](mode)(m)

	return m
}

// Delete preserves the key, including when zero is a stored numeric key.
// Storing an already linked deleted entry must not retarget the zero key.
func Test_J56StoreAfterEntryDeleteDoesNotOverwriteKeyZero(t *testing.T) {
	modes := []skiplistmap.SearchMode{
		skiplistmap.LenearSearchForBucket, skiplistmap.NestedSearchForBucket,
		skiplistmap.CombineSearch, skiplistmap.CombineSearch2, skiplistmap.CombineSearch3,
		skiplistmap.CombineSearch4, skiplistmap.NoItemSearchForBucket, skiplistmap.FalsesSearchForBucket,
	}
	for _, mode := range modes {
		for _, max := range []int{16, 1 << 20} {
			m := newJ56Map(mode, max)
			const k = 1
			zero := skiplistmap.NewEntryMap[skiplistmap.Uint64Key, any](0, "zero")
			e := skiplistmap.NewEntryMap[skiplistmap.Uint64Key, any](k, "v")
			for _, it := range []skiplistmap.MapItem[skiplistmap.Uint64Key, any]{zero, e} {
				if !m.StoreItem(it) {
					t.Fatalf("mode %d max %d: StoreItem(%v) failed", mode, max, it.Key())
				}
			}
			if _, ok := m.LoadItem(0); !ok {
				t.Fatalf("mode %d max %d: LoadItem(0) did not find the zero key", mode, max)
			}
			if v, ok := m.Get(0); !ok || v != "zero" {
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
				mode, max, stored, zero.Value(), skiplistmap.StepCheckLists[skiplistmap.Uint64Key, any](m))
		}
	}
}
