package skiplistmap

import (
	"fmt"
	"math/bits"
	"testing"
)

// oneNibbleKeys returns n keys whose hashes share the top nibble of their
// reverse, so that they go into one top bucket.
func oneNibbleKeys(n int) []string {
	byNibble := map[uint64][]string{}
	for i := 0; ; i++ {
		k := fmt.Sprintf("s%d", i)
		hash, _ := StringKey(k).KeyHash()
		nib := bits.Reverse64(hash) >> 60
		byNibble[nib] = append(byNibble[nib], k)
		if keys := byNibble[nib]; len(keys) == n {
			return keys
		}
	}
}

// After many splits, every bucket is on the list of its level once, and each
// list is in descending order of reverse: the place that insertOnLevel takes
// from the level above for a first down level is the right one.
func TestLevelListsStaySortedAfterSplits(t *testing.T) {
	h := New[StringKey, int](UseEmbeddedPool[StringKey, int](true), MaxPefBucket[StringKey, int](16), BucketMode[StringKey, int](CombineSearch3))
	for i, k := range oneNibbleKeys(400) {
		if !h.Set(StringKey(k), i) {
			t.Fatalf("Set %s failed", k)
		}
	}
	for i := 0; i < 4000; i++ {
		if !h.Set(StringKey(string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))), i) {
			t.Fatalf("Set %d failed", i)
		}
	}

	onList := map[*bucket[StringKey, int]]int32{}
	for level := int32(1); level <= 16; level++ {
		prev := ^uint64(0)
		first := true
		for cur := h.levelBucket(level).LevelHead.Next(); !cur.Empty(); cur = cur.Next() {
			b := bucketFromLevelHead[StringKey, int](cur)
			if b.level() != level {
				t.Fatalf("level %d: bucket %x of level %d on the list", level, b.reverse, b.level())
			}
			if !first && b.reverse >= prev {
				t.Fatalf("level %d: bucket %x after %x", level, b.reverse, prev)
			}
			if _, seen := onList[b]; seen {
				t.Fatalf("level %d: bucket %x twice", level, b.reverse)
			}
			onList[b] = level
			prev, first = b.reverse, false
		}
	}

	var missing int
	var visit func(b *bucket[StringKey, int])
	visit = func(b *bucket[StringKey, int]) {
		if b.level() > 0 {
			if _, ok := onList[b]; !ok {
				t.Logf("bucket %x of level %d is on no list", b.reverse, b.level())
				missing++
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
	if missing > 0 {
		t.Fatalf("%d buckets are on no list of their level", missing)
	}
	if len(onList) < 100 {
		t.Fatalf("only %d buckets were made; the test needs splits on several levels", len(onList))
	}
}
