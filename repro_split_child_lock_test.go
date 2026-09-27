//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_SplitOfNewBucketWaitsForItsLock replays this order in a map with the
// embedded pool and at most 3 entries in a bucket:
//
//  1. The top bucket 3 holds four keys whose second digits of the reversed
//     hash are 9, a, d and e. Its split at 0x38 would leave no key below, so
//     it is not split.
//  2. G1 sets a key of second digit 1 under the muPool of bucket 3. The split
//     at 0x38 moves 9, a, d and e to a new bucket b, publishes b and stops at
//     makeBucket2.recurse, before it splits b, which is over the limit.
//  3. G2 sets a key of second digit b: it locks the muPool of b and stops at
//     insertToPool.publish, before it stores the new array of b.
//  4. G1 resumes. It must not split b while G2 holds the lock of b: the
//     array that G2 stores afterwards would drop the split.
func Test_SplitOfNewBucketWaitsForItsLock(t *testing.T) {
	m := newEmbeddedMap(3)
	keys := []string{
		regionKeys(0x3, 0x9, 1)[0],
		regionKeys(0x3, 0xa, 1)[0],
		regionKeys(0x3, 0xd, 1)[0],
		regionKeys(0x3, 0xe, 1)[0],
		regionKeys(0x3, 0x1, 1)[0],
		regionKeys(0x3, 0xb, 1)[0],
	}
	for _, k := range keys[:4] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}

	s := newStepper(t)
	recurse := s.stopAt("map.makeBucket2.recurse", nil)
	done1 := goStep(t, func() { m.Set(keys[4], &list_head.ListHead{}) })
	recurse.waitReached(t, done1)

	publish := s.stopAt("map.insertToPool.publish", nil)
	done2 := goStep(t, func() { m.Set(keys[5], &list_head.ListHead{}) })
	publish.waitReached(t, done2)

	recurse.Release()
	if waitAtMost(done1, 200*time.Millisecond) {
		t.Errorf("G1 split b while G2 held the lock of b")
	}
	publish.Release()
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")

	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
	if err := skiplistmap.StepCheckBuckets(m.base); err != nil {
		t.Errorf("buckets broken: %v", err)
	}
}
