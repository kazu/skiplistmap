//go:build stephook

package skiplistmap_test

import (
	"sort"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Before the fix, the split of a bucket of the embedded pool (makeBucket2)
// shrank the items of the old bucket to the split point inside _split, before
// it linked the new bucket by addBucket. A lookup that had found the old
// bucket and searched it in that window saw only the items below the split
// point, and the fallback that looks at the bucket above the old one did not
// see the new bucket, which was not linked yet: the lookup missed a key that
// the map holds.
//
// Keys: 40 keys with the top 4 bits 3 of the reversed hash in a map with the
// embedded pool and at most 16 items per bucket. last is the key with the
// highest reversed hash. The map holds last and 9 other keys in the pool of
// the top bucket B of 0x3.., which has not split.
//
// Interleaving: G1 looks up last: it finds B and stops at bsearch.begin,
// before it reads the pool of B and its length. G2 sets the other 30 keys one
// by one; one of them splits B, and G2 stops at insertBucket.begin of the new
// bucket N, after the items from the split point, last among them, were
// copied into the pool of N and before N is linked. G1 resumes and searches
// B. Before the fix B holds only the items below the split point, and Get
// misses last. After the fix B keeps its items until N is linked, and Get
// finds last in B.
func Test_ReproJ9LookupMissesKeyWhileSplitLinksNewBucket(t *testing.T) {
	var keys []string
	for i := 0; len(keys) < 40; i++ {
		if k := crashKey(i); reverseOf(k)>>60 == 3 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })
	last := keys[len(keys)-1]
	stored := append([]string{last}, keys[:9]...)
	rest := keys[9 : len(keys)-1]

	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)))
	for _, k := range stored {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	if found, base := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(last)); found != base {
		t.Fatalf("the bucket of %q has split before the test", last)
	}

	s := newStepper(t)
	lookup := s.stopAt("map.bsearch.begin", nil)
	var got bool
	done1 := goStep(t, func() { _, got = m.base.Get(skiplistmap.StringKey(last)) })
	lookup.waitReached(t, done1)
	t.Logf("the lookup of last found the bucket %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](lookup.a))

	link := s.stopAt("map.insertBucket.begin", nil)
	done2 := goStep(t, func() {
		for _, k := range rest {
			m.Set(k, &list_head.ListHead{})
		}
	})
	link.waitReached(t, done2)
	if rn := skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](link.a); rn > reverseOf(last) {
		t.Fatalf("the new bucket %016x is above last %016x", rn, reverseOf(last))
	} else {
		t.Logf("the split links the new bucket %016x, which holds last %016x", rn, reverseOf(last))
	}

	lookup.Release()
	waitDone(t, done1, "Get(last)")
	if !got {
		t.Errorf("Get(%q) missed the key while the split linked the new bucket", last)
	}

	link.Release()
	waitDone(t, done2, "the Sets that split")
	assertStoredInOrder(t, m, keys)
}
