//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// A split links the dummy of the new bucket with add2, like any other entry.
// When the order check of add2 rejects the dummy, add2 must still link it:
// a bucket whose dummy is not linked makes makeBucket panic with "bucket
// head empty".
//
// Keys: four lower keys L below 0x38.. and one upper key U above 0x3c.. in
// the region 0x3, and X between 0x38.. and U. With MaxPefBucket(4) the store
// of U splits the bucket 0x30.. at b (reverse 0x38..).
//
// Interleaving: G1 stores U and stops at insertBucket.begin of b, then at
// add2.found for the dummy D of b, with U as the position: the order check
// will read the prev of U. G2 stores X: the split already moved the count of
// the keys below b out of the bucket 0x30.., so X splits nothing, and X is
// linked between the last L and U. G1 resumes: the prev of U is X, larger
// than D, so the order check rejects D. Before the fix add2 logs "fail
// insert" and returns true, D stays unlinked, and makeBucket panics with
// "bucket head empty" after b.head() read the empty dummy 100 times.
func Test_ReproJ3RejectedDummyMakesBucketHeadEmpty(t *testing.T) {
	const top = 0x3
	lower := regionKeys(top, 0x2, 4)
	upper := regionKeys(top, 0xc, 1)
	between := regionKeys(top, 0x9, 1)
	keys := append(append(append([]string{}, lower...), upper...), between...)
	items := newStepItems(keys)
	lowerItems, u, x := items[:4], &items[4], &items[5]

	m := newWrapHMap(skiplistmap.NewHMap())
	skiplistmap.MaxPefBucket(4)(m.base)
	skiplistmap.BucketMode(skiplistmap.CombineSearch4)(m.base)
	for i := range lowerItems {
		m.base.StoreItem(&lowerItems[i])
	}

	s := newStepper(t)
	begin := s.stopAt("map.insertBucket.begin", nil)
	done := goStep(t, func() { m.base.StoreItem(u) })
	begin.waitReached(t, done)
	b := begin.a
	t.Logf("split: %016x", skiplistmap.StepBucketReverse(b))
	if r := skiplistmap.StepBucketReverse(b) >> 56; r != 0x38 {
		begin.Release()
		t.Fatalf("the store of U split at %02x.., want 38..", r)
	}
	found := s.stopAt("map.add2.found", isNode(skiplistmap.StepBucketDummy(b)))
	begin.Release()
	found.waitReached(t, done)
	if found.b != nodeOf(u) {
		found.Release()
		t.Fatalf("add2 found %p as the position of the dummy, want U %p", found.b, nodeOf(u))
	}

	m.base.StoreItem(x)
	if n := s.total("map.makeBucket.claimed"); n != 1 {
		found.Release()
		t.Fatalf("the stores claimed %d buckets, want only the split of U", n)
	}
	found.Release()
	waitDone(t, done, "StoreItem(U)")
	t.Logf("add2 found a position for the dummy %d times", s.count("map.add2.found", skiplistmap.StepBucketDummy(b)))

	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}
