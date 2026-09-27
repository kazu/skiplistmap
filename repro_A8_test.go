//go:build stephook

package skiplistmap_test

import (
	"math/bits"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// a8Item is a user-defined item whose KeyHash returns k and c as they are,
// so that a test can choose the reversed hash of an item.
type a8Item struct {
	skiplistmap.SampleItem
	k, c uint64
}

func (x *a8Item) KeyHash() (uint64, uint64) { return x.k, x.c }

func (x *a8Item) HmapEntryFromListHead(l *elist_head.ListHead) skiplistmap.HMapEntry {
	return (*a8Item)(unsafe.Pointer(skiplistmap.SampleItemFromListHead(l)))
}

func newA8Item(key string, r, c uint64) *a8Item {
	x := &a8Item{k: bits.Reverse64(r), c: c}
	x.K = key
	x.SetValue(key)
	return x
}

// Test_ReproA8StoreNotFoundPositionCountsFailedInsert stores M, whose reversed
// hash is ^uint64(0), after A, in one goroutine.
//
//  1. StoreItem(A) links A (reverse 0xf000000000000001) after the dummy of
//     the bucket 0xf0.., before the last dummy (reverse ^uint64(0)).
//  2. StoreItem(M): _set starts the search at A. add2 looks for the first
//     entry whose reverse is larger than ^uint64(0), finds none, and takes the
//     path for a position that is not found. With a bucket given, it takes the
//     entry after the dummy of the bucket, A, as the entry to link M before.
//     inserBeforeWithCheck(A, M) (map.add2.bucketInsert with M and A) fails
//     with "invalid insert order" because A comes before M. add2 drops the
//     error and returns true, and _set adds 1 to the length.
//
// M is not linked: LoadItemByHash does not find M, while StoreItem(M)
// returned true and Len is 2.
func Test_ReproA8StoreNotFoundPositionCountsFailedInsert(t *testing.T) {
	s := newStepper(t)
	m := skiplistmap.New(
		skiplistmap.MaxPefBucket(1<<20),
		skiplistmap.BucketMode(skiplistmap.CombineSearch4),
		skiplistmap.ItemFn(func() skiplistmap.MapItem { return (*a8Item)(nil) }),
	)
	a := newA8Item("a", 0xf000000000000001, 1)
	if !m.StoreItem(a) {
		t.Fatalf("StoreItem(a) returned false")
	}
	if _, found := m.LoadItemByHash(a.k, a.c); !found {
		t.Fatalf("a is not found after StoreItem(a)")
	}

	mItem := newA8Item("m", ^uint64(0), 1)
	var stored bool
	runWithDeadline(t, 10*time.Second, func() { stored = m.StoreItem(mItem) })

	n := s.count("map.add2.bucketInsert", nodeOf(&mItem.SampleItem))
	_, right := s.args("map.add2.bucketInsert")
	t.Logf("add2.bucketInsert of m: %d times, right is a: %v", n, right == nodeOf(&a.SampleItem))

	_, found := m.LoadItemByHash(mItem.k, mItem.c)
	linked := 0
	m.RangeItem(func(item skiplistmap.MapItem) bool {
		linked++
		return true
	})
	t.Logf("StoreItem(m) = %v, found = %v, Len = %d, items in the list = %d", stored, found, m.Len(), linked)
	if stored && !found {
		t.Fatalf("StoreItem(m) returned true and Len is %d, but m is not found (%d items in the list)", m.Len(), linked)
	}
	if m.Len() != linked {
		t.Fatalf("Len is %d, but %d items are in the list", m.Len(), linked)
	}
}

// Test_ReproA8InsertBeforeGivesUpNextToUnfinishedInsert stores M, whose
// reversed hash is ^uint64(0), into an empty map while X (reverse
// 0xf800000000000000) is being linked just before the last dummy L.
//
//  1. GM: StoreItem(M). add2 finds no position for M and, with the bucket
//     0xf0.. given, takes L, the entry after the dummy D of the bucket, as the
//     entry to link M before (map.add2.bucketInsert with M and L).
//     inserBeforeWithCheck(L, M) passes its checks (D <= M <= L). GM stops at
//     elist insert.begin(M, L) of InsertBefore, before its first CAS.
//  2. GX: StoreItem(X). add2 finds L as the position of X, and insertInOrder
//     links X between D and L. GX stops at elist add.cas2 of X: D.next is X,
//     and L.prev is still D.
//  3. GM resumes. elist insertBefore reads D as the prev of L, and its CAS of
//     D.next from L to M fails because D.next is X. It reads the prev of L
//     again, D, and fails the same way, 100 times, then gives up without an
//     error. InsertBefore returns no error, add2 returns true, and _set adds
//     1 to the length.
//  4. GX resumes and finishes linking X.
//
// M is not linked: LoadItemByHash does not find M, while StoreItem(M)
// returned true and Len is 2.
func Test_ReproA8InsertBeforeGivesUpNextToUnfinishedInsert(t *testing.T) {
	m := skiplistmap.New(
		skiplistmap.MaxPefBucket(1<<20),
		skiplistmap.BucketMode(skiplistmap.CombineSearch4),
		skiplistmap.ItemFn(func() skiplistmap.MapItem { return (*a8Item)(nil) }),
	)
	mItem := newA8Item("m", ^uint64(0), 1)
	x := newA8Item("x", 0xf800000000000000, 1)
	mNode, xNode := nodeOf(&mItem.SampleItem), nodeOf(&x.SampleItem)

	s := newStepper(t)
	var rightOfM, nextOfX unsafe.Pointer
	stopM := s.stopAt("elist.insert.begin", func(a, b, c unsafe.Pointer) bool {
		if a != mNode {
			return false
		}
		rightOfM = c
		return true
	})
	var storedM, storedX bool
	doneM := goStep(t, func() { storedM = m.StoreItem(mItem) })
	stopM.waitReached(t, doneM)
	if r := skiplistmap.StepEntryReverse(rightOfM); r != ^uint64(0) {
		t.Fatalf("M is inserted before an entry of reverse %x, not the last dummy", r)
	}

	stopX := s.stopAt("elist.add.cas2", func(a, b, c unsafe.Pointer) bool {
		if a != xNode {
			return false
		}
		nextOfX = c
		return true
	})
	doneX := goStep(t, func() { storedX = m.StoreItem(x) })
	stopX.waitReached(t, doneX)
	if nextOfX != rightOfM {
		t.Fatalf("X is linked before an entry of reverse %x, not before the entry M is inserted before",
			skiplistmap.StepEntryReverse(nextOfX))
	}

	_, right := s.args("map.add2.bucketInsert")
	t.Logf("add2.bucketInsert of M: %d times, right is the last dummy: %v",
		s.count("map.add2.bucketInsert", mNode), right == rightOfM)

	// X resumes only after M either returned or retried more often than the
	// 100 retries of InsertBefore.
	stopM.Release()
	deadline := time.Now().Add(10 * time.Second)
	for s.count("elist.add.cas1", mNode) <= 100 {
		select {
		case <-doneM:
		default:
			if time.Now().After(deadline) {
				t.Fatalf("StoreItem(m) neither finished nor retried")
			}
			runtime.Gosched()
			continue
		}
		break
	}
	t.Logf("CASes of M tried while X is stopped: %d", s.count("elist.add.cas1", mNode))
	stopX.Release()
	waitDone(t, doneM, "StoreItem(m)")
	waitDone(t, doneX, "StoreItem(x)")

	_, foundM := m.LoadItemByHash(mItem.k, mItem.c)
	_, foundX := m.LoadItemByHash(x.k, x.c)
	linked := 0
	m.RangeItem(func(item skiplistmap.MapItem) bool {
		linked++
		return true
	})
	t.Logf("StoreItem(m) = %v, found = %v; StoreItem(x) = %v, found = %v; Len = %d, items in the list = %d",
		storedM, foundM, storedX, foundX, m.Len(), linked)
	if storedM && !foundM {
		t.Fatalf("StoreItem(m) returned true and Len is %d, but m is not found (%d items in the list)", m.Len(), linked)
	}
	if m.Len() != linked {
		t.Fatalf("Len is %d, but %d items are in the list", m.Len(), linked)
	}
	if err := skiplistmap.StepCheckLists(m); err != nil {
		t.Fatalf("lists are broken: %v", err)
	}
}
