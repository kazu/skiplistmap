package skiplistmap

import (
	"fmt"
	"math/bits"
	"testing"
)

// A slot that a split has claimed but not built yet has no item pool, so a
// lookup of its keys must not find it and must reach the bucket that still
// holds the items.
func Test_ClaimedBucketIsNotFound(t *testing.T) {
	h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true))
	parent := &h.buckets[3]
	r := parent.reverse | 8<<56
	if b := h.bucketFromPoolEmbedded(r); b == nil || b == parent {
		t.Fatalf("bucketFromPoolEmbedded(%x) did not claim a new slot", r)
	}
	if got := h.findBucket(r); got.toBase() != parent {
		t.Errorf("findBucket(%x) = bucket %x of level %d, want the parent %x", r, got.reverse, got.level(), parent.reverse)
	}
}

// A lookup that found its bucket before a split moved the key to a new
// bucket must still find the key.
func Test_LookupFromBucketFoundBeforeSplit(t *testing.T) {
	h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true))
	MaxPefBucket[StringKey, any](16)(h)
	var keys []string
	for i := 0; len(keys) < 200; i++ {
		k := fmt.Sprintf("%d", i)
		if bits.Reverse64(MemHashString(k))>>60 == 3 {
			keys = append(keys, k)
		}
	}
	last := keys[0]
	for _, k := range keys {
		if bits.Reverse64(MemHashString(k)) > bits.Reverse64(MemHashString(last)) {
			last = k
		}
	}
	rev := bits.Reverse64(MemHashString(last))
	h.Set(StringKey(last), 1)
	found := h.findBucket(rev)
	for _, k := range keys {
		if k == last {
			continue
		}
		h.Set(StringKey(k), 1)
		if h.findBucket(rev).toBase() != found.toBase() {
			break
		}
	}
	if h.findBucket(rev).toBase() == found.toBase() {
		t.Fatalf("no split moved %q", last)
	}
	if e := h.bsearchBybucket(found, rev, true); e == nil {
		t.Errorf("lookup of %q from the bucket found before the split missed", last)
	}
}
