//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// Two items m1 and m2 of one key whose reversed hash is ^uint64(0), in a map
// without the embedded pool that never splits a bucket, with no entry in the
// bucket 0xf0... add2 looks for the first entry whose reverse is larger than
// ^uint64(0), finds none, and takes the path for a position that is not
// found: with the bucket given, it links the item before the entry after the
// dummy of the bucket, here the last dummy.
//
// G2 stores m2: its lookup misses the key, and it stops at add2.bucketInsert
// with the last dummy as the entry to link m2 before. The main goroutine
// stores m1 to the end: its lookup misses the key too, since m2 is not
// linked, and it links m1 before the last dummy. When G2 resumes, nothing
// looks up the key again: the order check accepts m1 as the left neighbour of
// m2, since their reversed hashes are equal, and m2 is linked after m1. The
// map then holds the one key twice: both StoreItems return true, Len is 2,
// and RangeItem yields both items.
func Test_J51StoreSameNewKeyNoPositionLinkedTwice(t *testing.T) {
	m := skiplistmap.New[fixedHashKey, any](skiplistmap.MaxPefBucket[fixedHashKey, any](1<<20), skiplistmap.BucketMode[fixedHashKey, any](skiplistmap.CombineSearch4))
	m1 := newA8Item("same", ^uint64(0), 1)
	m2 := newA8Item("same", ^uint64(0), 1)

	s := newStepper(t)
	stop := s.stopAt("map.add2.bucketInsert", isNode(nodeOf(m2)))
	var ok1, ok2 bool
	done := goStep(t, func() { ok2 = m.StoreItem(m2) })
	stop.waitReached(t, done)
	runWithDeadline(t, 10*time.Second, func() { ok1 = m.StoreItem(m1) })
	t.Logf("StoreItem(m1) reached add2.bucketInsert %d times", s.count("map.add2.bucketInsert", nodeOf(m1)))
	stop.Release()
	waitDone(t, done, "StoreItem(m2)")

	var linked []string
	runWithDeadline(t, 10*time.Second, func() {
		m.RangeItem(func(item skiplistmap.MapItem[fixedHashKey, any]) bool {
			linked = append(linked, item.Key().name)
			return len(linked) < 16
		})
	})
	t.Logf("StoreItem(m1) = %v, StoreItem(m2) = %v, Len = %d, RangeItem = %q", ok1, ok2, m.Len(), linked)
	if len(linked) != 1 {
		t.Errorf("the map holds the one key %d times: %q", len(linked), linked)
	}
	if n := m.Len(); n != 1 {
		t.Errorf("Len = %d after storing two items of one key, want 1", n)
	}
	runtime.KeepAlive(m1)
	runtime.KeepAlive(m2)
}

// The other path for a position that is not found links the item before the
// node before the tail (add2.tailInsert). add2 takes it only when _set gives
// it no bucket, or a bucket whose entry is nil. In a map without the embedded
// pool, bucket._head returns the dummy of the bucket and is never nil, and
// the entry of the bucket is nil only while its dummy is not linked, that is,
// inside a split, before insertBucket.dummyLinked.
//
// This test looks for such a store. In a map that splits at 2 items per
// bucket, it stores keys of one region until a store stops inside the first
// split of the region, at each step point of the split in turn. While that
// store is stopped, it stores the rest of the keys of the region and two
// items of the key whose reversed hash is ^uint64(0), each in its own
// goroutine, and gives each of them 500ms; a store that waits for the split
// runs on after it. No store may reach add2.tailInsert. This covers the step
// points of one split, not every interleaving.
func Test_J51TailPathNotTakenDuringSplit(t *testing.T) {
	points := []string{
		"map.makeBucket.begin", "map.makeBucket.pairFound", "map.makeBucket.claimed",
		"map.bucketFromPool.levelFound", "map.bucketFromPool.lenStored",
		"map.makeBucket.beforeInit", "map.insertBucket.begin", "map.insertBucket.dummyLinked",
		"map.makeBucket.added", "map.makeBucket.levelFound",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			keys := regionKeys(0x3, 0xc, 16)
			items := make([]skiplistmap.Entry[fixedHashKey, any], len(keys))
			for i, key := range keys {
				hash, conflict := skiplistmap.StringKey(key).KeyHash()
				items[i].InitEntry(fixedHashKey{key, hash, conflict}, key)
			}
			m1 := newA8Item("same", ^uint64(0), 1)
			m2 := newA8Item("same", ^uint64(0), 1)
			m := skiplistmap.NewHMap[fixedHashKey, any]()
			skiplistmap.MaxPefBucket[fixedHashKey, any](2)(m)
			skiplistmap.BucketMode[fixedHashKey, any](skiplistmap.CombineSearch4)(m)

			s := newStepper(t)
			stop := s.stopAt(point, nil)
			_, next, done := storeTypedUntilStop(t, m, items, stop)
			var dones []<-chan struct{}
			blocked := 0
			store := func(it skiplistmap.MapItem[fixedHashKey, any]) {
				d := goStep(t, func() { m.StoreItem(it) })
				dones = append(dones, d)
				if !waitAtMost(d, 500*time.Millisecond) {
					blocked++
				}
			}
			for i := next; i < len(items); i++ {
				store(&items[i])
			}
			store(m1)
			store(m2)
			tails := s.total("map.add2.tailInsert")
			stop.Release()
			waitDone(t, done, "the stopped store")
			for _, d := range dones {
				waitDone(t, d, "a store")
			}
			t.Logf("%s: %d stores while the split was stopped, %d of them waited for it, add2.tailInsert %d times",
				point, len(items)-next+2, blocked, tails)
			if tails != 0 {
				t.Errorf("a store reached add2.tailInsert while a split was stopped at %s", point)
			}
			runtime.KeepAlive(items)
			runtime.KeepAlive(m1)
			runtime.KeepAlive(m2)
		})
	}
}
