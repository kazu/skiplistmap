package skiplistmap_test

import (
	"math/bits"
	"runtime"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// fixedHashItem is a user-defined item whose KeyHash returns k and c as they
// are, so that two items can share the hash k and differ in the conflict c.
type fixedHashItem struct {
	skiplistmap.SampleItem
	k, c uint64
}

func (x *fixedHashItem) KeyHash() (uint64, uint64) { return x.k, x.c }

func (x *fixedHashItem) HmapEntryFromListHead(l *elist_head.ListHead) skiplistmap.HMapEntry {
	return (*fixedHashItem)(unsafe.Pointer(skiplistmap.SampleItemFromListHead(l)))
}

// storeSameReverse stores a and b, two items of the hash k whose reversed
// value is r, with the conflicts 1 and 2, in a new map.
func storeSameReverse(t *testing.T, r uint64) (m *skiplistmap.Map, a, b *fixedHashItem) {
	t.Helper()
	m = skiplistmap.New(skiplistmap.ItemFn(func() skiplistmap.MapItem { return (*fixedHashItem)(nil) }))
	k := bits.Reverse64(r)
	a = &fixedHashItem{k: k, c: 1}
	a.K = "a"
	a.SetValue(1)
	b = &fixedHashItem{k: k, c: 2}
	b.K = "b"
	b.SetValue(2)
	m.StoreItem(a)
	m.StoreItem(b)
	if n := m.Len(); n != 2 {
		t.Fatalf("Len() = %d, want 2", n)
	}
	var got []*fixedHashItem
	m.RangeItem(func(item skiplistmap.MapItem) bool {
		got = append(got, item.(*fixedHashItem))
		return true
	})
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("RangeItem = %v, want a then b", got)
	}
	return m, a, b
}

// sameReverseCases are reversed hashes on both sides of a bucket. The search
// starts from the nearer of the two buckets around the key. Just above a
// bucket it walks forward from the lower bucket and returns the first node
// of the reversed hash; at 0x?a5a... it walks backward from the upper bucket
// and returns the last one.
var sameReverseCases = []struct {
	name string
	r    uint64
}{
	{"forward", 0x3000000000000101},
	{"backward", 0x3a5a5a5a5a5a5a5b},
}

// A single goroutine stores a (conflict 1) and then b (conflict 2) with the
// same hash. Insertion orders nodes by the reversed hash only, so b is linked
// right after a, and both are in the list. A lookup walks to one end of the
// run of that reversed hash and compares the conflict of that one node only:
// searching forward it returns a, so b is not found; searching backward it
// returns b, so a is not found.
func Test_Repro_3_1_5_SameReverseDifferentConflictBothFound(t *testing.T) {
	for _, tc := range sameReverseCases {
		t.Run(tc.name, func(t *testing.T) {
			m, a, b := storeSameReverse(t, tc.r)
			for _, x := range []*fixedHashItem{a, b} {
				got, ok := m.LoadItemByHash(x.k, x.c)
				if !ok || got.(*fixedHashItem) != x {
					t.Errorf("LoadItemByHash(%#x, conflict %d) = %v, %v; want %q", x.k, x.c, got, ok, x.K)
				}
			}
			runtime.KeepAlive(a)
			runtime.KeepAlive(b)
		})
	}
}

// The same two items, with the one at the end the search reaches first
// deleted: a when searching forward, b when searching backward. The search
// skips the deleted node and returns the other one. This passes now; it
// keeps a fix that looks through the whole run from breaking the skip of
// deleted nodes.
func Test_Repro_3_1_5_SameReverseSkipsDeleted(t *testing.T) {
	for _, tc := range sameReverseCases {
		t.Run(tc.name, func(t *testing.T) {
			m, a, b := storeSameReverse(t, tc.r)
			del, keep := a, b
			if tc.name == "backward" {
				del, keep = b, a
			}
			del.Delete()
			m.AddLen(-1)
			got, ok := m.LoadItemByHash(keep.k, keep.c)
			if !ok || got.(*fixedHashItem) != keep {
				t.Errorf("LoadItemByHash(%#x, conflict %d) = %v, %v; want %q", keep.k, keep.c, got, ok, keep.K)
			}
			if _, ok := m.LoadItemByHash(del.k, del.c); ok {
				t.Errorf("LoadItemByHash(%#x, conflict %d) found the deleted %q", del.k, del.c, del.K)
			}
			runtime.KeepAlive(a)
			runtime.KeepAlive(b)
		})
	}
}
