package skiplistmap

import "testing"

// MinCapItems is the least capacity of the pool of a bucket after it grows,
// and at least that of its first array, so that a pool of a bucket that holds
// up to MaxPefBucket entries grows once instead of doubling up to it.
func TestMinCapItemsSetsThePoolCapacity(t *testing.T) {
	for _, tc := range []struct {
		min, first, grown int
	}{
		{0, CntOfPersamepleItemPool, 4},
		{128, 128, 128},
	} {
		var opts []OptHMap[StringKey, int]
		opts = append(opts, UseEmbeddedPool[StringKey, int](true))
		if tc.min > 0 {
			opts = append(opts, MinCapItems[StringKey, int](tc.min))
		}
		h := New[StringKey, int](opts...)
		h.Set("k", 1)
		var pool *samepleItemPool[StringKey, int]
		for i := range h.buckets {
			if p := h.buckets[i]._itemPool; p != nil && p.ptrItems().Len() == 1 {
				pool = p
			}
		}
		if pool == nil {
			t.Fatalf("min=%d: no bucket holds the entry", tc.min)
		}
		// the top buckets share their first array, so a bucket can hold a
		// part of it; with the option, that array is at least min long
		if tc.min > 0 && pool.ptrItems().Cap() < tc.first {
			t.Fatalf("min=%d: first capacity = %d, want at least %d (pool.minCap=%d h.minCapItems=%d)", tc.min, pool.ptrItems().Cap(), tc.first, pool.minCap, h.minCapItems)
		}
		if got := poolCap(3, pool.minCapItems()); got != tc.grown {
			t.Fatalf("min=%d: capacity after growing from 3 = %d, want %d", tc.min, got, tc.grown)
		}
	}
}
