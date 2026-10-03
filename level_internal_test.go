package skiplistmap

import (
	"testing"
)

// A split puts its new bucket on the list of buckets before it puts the
// LevelHead of the bucket on the list of its level; until then the LevelHead
// lies between sentinels of its own. The position that nextOnLevelOf gives for
// a bucket just before it must not be that LevelHead: a bucket linked there
// would be out of the list of the level.
func Test_NextOnLevelSkipsABucketNotOnTheLevelList(t *testing.T) {
	h := New[StringKey, any]()
	h.Set("k", 1)
	var b2 *bucket[StringKey, any]

	for i := range h.buckets {
		if h.buckets[i].reverse > 0 && h.buckets[i].level() == 1 {
			b2 = &h.buckets[i]
			break
		}
	}
	if b2 == nil {
		t.Fatalf("no bucket of level 1 with a reverse above 0")
	}
	nb := newBucket[StringKey, any]()
	nb.reverse = b2.reverse - 1
	nb.setLevel(1)
	nb.Init()
	nb.LevelHead.Init()
	if err := h.addBucket(nb); err != nil {
		t.Fatalf("addBucket: %v", err)
	}
	if b2.nextAsB() != nb {
		t.Fatalf("the new bucket is not after %x on the list of buckets", b2.reverse)
	}
	if pos := h.nextOnLevelOf(b2, 1); pos == &nb.LevelHead {
		t.Errorf("nextOnLevelOf gave the LevelHead of a bucket that is not on the list of its level")
	}
}
