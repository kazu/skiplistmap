//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime/debug"
	"sort"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

type a9Item = skiplistmap.Entry[fixedHashKey, any]

func newA9Item(key string, r uint64) *a9Item {
	return skiplistmap.NewEntry[fixedHashKey, any](fixedHashKey{key, bits.Reverse64(r), 1}, key)
}

// a9Claimed returns the reverses of the buckets that makeBucket claimed so
// far, in ascending order.
func a9Claimed(s *stepper) []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rs []uint64
	for h := range s.hits {
		if h.point == "map.makeBucket.claimed" && h.a != nil {
			rs = append(rs, skiplistmap.StepBucketReverse[fixedHashKey, any](h.a))
		}
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
	return rs
}

func a9IsBucket(r uint64) func(a, b, c unsafe.Pointer) bool {
	return func(a, b, c unsafe.Pointer) bool {
		return a != nil &&
			skiplistmap.StepBucketReverse[fixedHashKey, any](a) == r
	}
}

// Test_ReproA9StoreIntoChildOfUnfinishedBucketPanics stores items by
// StoreItem only, as Test_ConcurrentStoreItem does, into a map that splits a
// bucket as soon as it holds 2 items. The reverses below are written as their
// top 12 or 16 bits; the low bits of each item are 1.
//
// Setup, in one goroutine: 0x39 and 0x3a make bucket 0x38 (the first
// downLevels of bucket 0x3), 0x35 and 0x36 make 0x34, and 0x32 makes 0x32.
// 0x34 and 0x32 are claimed by the CAS on the state of an existing element of
// the downLevels, which sets the level of the claimed bucket to
// -(level of the parent + 1), and makeBucket turns it positive after
// addBucket.
//
//  1. G_P stores 0x308. makeBucket computes 0x31 from the pair (0x3, 0x32)
//     and claims P = 0x31 with level -2. addBucket links P and its dummy.
//     G_P stops at makeBucket.added, before it turns the level of P to 2.
//  2. G2 stores 0x318. It is linked after the dummy of P, and makeBucket
//     computes 0x318 from the pair (P, 0x32). bucketFromPool passes P (its
//     state is not active and 0x318 is one level below it), makes the
//     downLevels of P with level P.level()+1 = -1, and claims C = 0x318.
//     makeBucket links C, and onOk sets the length of the downLevels of P
//     to 9. G2 finishes.
//  3. G3 stores 0x312. It is linked after the dummy of P, and makeBucket
//     computes 0x314 from the pair (P, C). bucketFromPool passes P and claims
//     M = 0x314, the element 4 of the downLevels of P, by the CAS on its
//     state, and sets its level to -(P.level()+1) = -(-2+1) = +1 while its
//     dummy is not linked. G3 stops at makeBucket.claimed.
//  4. G_P runs to the end and sets the level of P to 2.
//  5. StoreItem(K), K = 0x3148. It first looks K up: _loadItem,
//     getWithBucket and searchBucket4update call findBucket, and _findBucket
//     goes to P (level 2) and then to M (level 1), and returns M. M has no
//     item pool, so searchBucket4update reads M.prevAsB(). M is not linked,
//     so its prev is nil, and StoreItem(K) panics with a nil pointer
//     dereference.
//  6. G3 runs to the end and links M.
//
// StoreItem(K) panics and K is not stored.
func Test_ReproA9StoreIntoChildOfUnfinishedBucketPanics(t *testing.T) {
	s := newStepper(t)
	m := skiplistmap.New[fixedHashKey, any](skiplistmap.MaxPefBucket[fixedHashKey, any](1), skiplistmap.BucketMode[fixedHashKey, any](skiplistmap.CombineSearch4))
	const low = 1
	top := func(r uint64, bitsOf int) uint64 { return r<<(64-bitsOf) | low }

	var stored []*a9Item
	store := func(it *a9Item) {
		t.Helper()
		if !m.StoreItem(it) {
			t.Fatalf("StoreItem(%s) returned false", it.Key().name)
		}
		stored = append(stored, it)
	}
	for _, r := range []uint64{0x39, 0x3a, 0x35, 0x36, 0x32} {
		store(newA9Item(fmt.Sprintf("setup-%x", r), top(r, 8)))
	}
	t.Logf("claimed in setup: %x", a9Claimed(s))
	for _, r := range []uint64{0x38, 0x34, 0x32} {
		found := false
		for _, c := range a9Claimed(s) {
			found = found || c == top(r, 8)&^low
		}
		if !found {
			t.Fatalf("setup did not make bucket %x: claimed %x", r, a9Claimed(s))
		}
	}

	rP, rC, rM := top(0x31, 8)&^low, top(0x318, 12)&^low, top(0x314, 12)&^low

	// 1. G_P claims P and links it; it stops before the level of P is positive.
	stP := s.stopAt("map.makeBucket.added", a9IsBucket(rP))
	iP := newA9Item("p", top(0x308, 12))
	doneP := goStep(t, func() { m.StoreItem(iP) })
	stP.waitReached(t, doneP)
	stored = append(stored, iP)

	// 2. G2 makes C below P and finishes.
	i2 := newA9Item("c", top(0x318, 12))
	done2 := goStep(t, func() { m.StoreItem(i2) })
	waitDone(t, done2, "StoreItem(0x318)")
	stored = append(stored, i2)
	gotC := false
	for _, c := range a9Claimed(s) {
		gotC = gotC || c == rC
	}
	if !gotC {
		t.Fatalf("G2 did not claim %x: claimed %x", rC, a9Claimed(s))
	}

	// 3. G3 claims M with a positive level and stops before linking it.
	stM := s.stopAt("map.makeBucket.claimed", a9IsBucket(rM))
	i3 := newA9Item("m", top(0x312, 12))
	done3 := goStep(t, func() { m.StoreItem(i3) })
	stM.waitReached(t, done3)
	stored = append(stored, i3)

	// 4. G_P turns the level of P positive and finishes.
	stP.Release()
	waitDone(t, doneP, "StoreItem(0x308)")

	// 5. K goes into M, whose dummy is not linked.
	k := newA9Item("k", top(0x3148, 16))
	var kStored bool
	doneK := goStep(t, func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("StoreItem(k) panics: %v\n%s", r, debug.Stack())
				panic(r)
			}
		}()
		kStored = m.StoreItem(k)
	})
	waitDone(t, doneK, "StoreItem(0x3148)")
	stored = append(stored, k)

	// 6. G3 links M and finishes.
	stM.Release()
	waitDone(t, done3, "StoreItem(0x312)")

	linked := 0
	m.RangeItem(func(item skiplistmap.MapItem[fixedHashKey, any]) bool {
		linked++
		return true
	})
	t.Logf("StoreItem(k) = %v, Len = %d, stored = %d, items in the list = %d", kStored, m.Len(), len(stored), linked)
	var missing []string
	for _, it := range stored {
		if _, ok := m.LoadItemByHash(it.Key().k, it.Key().c); !ok {
			missing = append(missing, fmt.Sprintf("%s(%016x)", it.Key().name, bits.Reverse64(it.Key().k)))
		}
	}
	if len(missing) > 0 {
		t.Errorf("not found: %v (Len = %d, items in the list = %d)", missing, m.Len(), linked)
	}
	if m.Len() != len(stored) {
		t.Errorf("Len = %d, want %d", m.Len(), len(stored))
	}
	if err := skiplistmap.StepCheckLists[fixedHashKey, any](m); err != nil {
		t.Errorf("StepCheckLists: %v", err)
	}
}
