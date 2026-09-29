//go:build stephook && race

package skiplistmap_test

import (
	"io"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k1 < k3 < k4 < k5. G1 sets k2:
// insertToPool builds a new array with k2 in it, links it into the list and
// stops at insertToPool.publish, before CopyFrom stores the array into the
// pool. G2 then runs DumpBucket alone on the only P: for the top bucket 3,
// bucket.len reads the length of the pool with the plain read
// len(b.itemPool().items), and DumpBucket returns without taking a lock. G1
// then stores the new length of the pool with CompareAndSwapInt in CopyFrom.
// Nothing orders the read of G2 before the store of G1, so the race detector
// reports the two.
func Test_J35DumpBucketLenRacesCopyFrom(t *testing.T) {
	m, keys := n2Map(t, 6, 0, 1, 3, 4, 5)

	s := newStepper(t)
	stop1 := s.stopAt("map.insertToPool.publish", nil)
	done1 := goStep(t, func() { m.Set(keys[2], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.base.DumpBucket(io.Discard) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}

// A map with the embedded pool and at most 3 items per bucket holds three keys
// of the top bucket 3, k1 < k2 < k9, whose second digits of the reversed hash
// are 1, 2 and 9. G1 sets ka, whose second digit is a: the bucket is over the
// limit, makeBucket2 splits it at 0x38 into the top bucket 3 (k1, k2) and a
// new bucket b (k9, ka), publishes b, and stops at makeBucket2.recurse, before
// it checks the lengths of the two buckets. G1 holds the muPool of the top
// bucket 3, and b has its own muPool. G2 then sets k9', whose second digit is
// 9 and which lies between k9 and ka, alone on the only P: it locks the muPool
// of b, and insertToPool stores a new array into the pool of b with
// CompareAndSwapPointer in CopyFrom. G1 then reads the pool of b with the
// plain read len(b.itemPool().items) in bucket.len. Nothing orders the store
// of G2 before the read of G1, so the race detector reports the two.
func Test_J35SplitLenRacesCopyFromOfNewBucket(t *testing.T) {
	m := newEmbeddedMap(3)
	nines := regionKeys(0x3, 0x9, 2)
	keys := []string{
		regionKeys(0x3, 0x1, 1)[0],
		regionKeys(0x3, 0x2, 1)[0],
		nines[0],
		regionKeys(0x3, 0xa, 1)[0],
		nines[1],
	}
	for _, k := range keys[:3] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}

	s := newStepper(t)
	stop1 := s.stopAt("map.makeBucket2.recurse", nil)
	done1 := goStep(t, func() { m.Set(keys[3], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	done2 := runAloneThenRelease(t, stop1, func() { m.Set(keys[4], &list_head.ListHead{}) })
	waitDone(t, done2, "G2")
	waitDone(t, done1, "G1")
}
