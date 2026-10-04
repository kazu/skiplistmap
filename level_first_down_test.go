package skiplistmap

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap/atomic_util"
)

// firstDownOf gives b a down level array whose first element is the first
// down level of b, the way bucketFromPoolEmbedded makes it, without linking it
// on the list of its level.
func firstDownOf[K Key[K], V any](b *bucket[K, V]) *bucket[K, V] {
	downs := b.ptrDownLevels()
	arr := make([]bucket[K, V], 16)
	atomic.StorePointer(&downs.data, unsafe.Pointer(unsafe.SliceData(arr)))
	atomic_util.StoreInt(&downs.cap, 16)
	atomic_util.StoreInt(&downs.len, 1)
	f := downs.at(0)
	f.setLevel(b.childLevel())
	f.reverse = b.reverse
	f.Init()
	f.LevelHead.Init()
	f._parent = b
	return f
}

// The parent of a first down level can be a first down level itself, which is
// not on the list of buckets. nextOnLevelOf took that for the end of the list
// of buckets and gave up, so insertOnLevel walked the list of the level from
// its start for every such bucket. It has to walk from the nearest ancestor on
// the list of buckets, and when no bucket of the level follows, give the end of
// the list of the level instead of nil.
func Test_NextOnLevelClimbsToAnAncestorOnTheListOfBuckets(t *testing.T) {
	h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true))
	h.Set("k", 1)
	var b1 *bucket[StringKey, any]
	for i := range h.buckets {
		if h.buckets[i].reverse > 0 && h.buckets[i].level() == 1 {
			b1 = &h.buckets[i]
			break
		}
	}
	if b1 == nil {
		t.Fatalf("no bucket of level 1 with a reverse above 0")
	}

	// a bucket after b1 on the list of buckets, with a bucket of level 3 on
	// its chain of first down levels
	nb := newBucket[StringKey, any]()
	nb.reverse = b1.reverse - 1
	nb.setLevel(1)
	nb.Init()
	nb.LevelHead.Init()
	if err := h.addBucket(nb); err != nil {
		t.Fatalf("addBucket: %v", err)
	}
	if b1.nextAsB() != nb {
		t.Fatalf("the new bucket is not after %x on the list of buckets", b1.reverse)
	}
	n3 := firstDownOf(firstDownOf(nb))
	h.insertOnLevel(n3, 3, "test", nil)

	// b1 -> f2 -> f3: two first down levels, neither on the list of buckets
	f3 := firstDownOf(firstDownOf(b1))
	if f3._parent.nextAsB() != f3._parent {
		t.Fatalf("the parent of f3 is on the list of buckets")
	}
	if pos := h.nextOnLevelOf(f3, 3); pos != &n3.LevelHead {
		t.Fatalf("nextOnLevelOf(f3, 3) did not find the bucket of level 3 after the ancestor of f3")
	}

	// a level that no following bucket has: the end of the list of the level
	f4 := firstDownOf(f3)
	if pos := h.nextOnLevelOf(f4, 4); pos != h.levelEnds[3] {
		t.Fatalf("nextOnLevelOf(f4, 4) did not give the end of the list of level 4")
	}
	h.insertOnLevel(f4, 4, "test", nil)
	if first := h.levelBucket(4).LevelHead.Next(); first != &f4.LevelHead {
		t.Fatalf("f4 is not the first bucket of level 4")
	}
	if !f4.LevelHead.Next().Empty() {
		t.Fatalf("f4 is not the last bucket of level 4")
	}
}
