package skiplistmap

import (
	"math/bits"
	"sync/atomic"
	"testing"

	"github.com/kazu/elist_head"
)

type suffixSlideKey uint64

func (k suffixSlideKey) KeyHash() (uint64, uint64)       { return bits.Reverse64(uint64(k)), 1 }
func (k suffixSlideKey) Equal(other suffixSlideKey) bool { return k == other }

func TestPoolSlideKeepsPrefix(t *testing.T) {
	p, ends := makeInsertPool(8, 11)
	old := p.items
	e, _, _ := p.insertToPool(13, nil)
	if p.items.data != old.data || p.items.Cap() != old.Cap() {
		t.Fatal("suffix fits, but insertion did not keep the existing array")
	}
	if p.items.Len() != 11 || e != p.items.at(8) {
		t.Fatal("wrong suffix insertion range")
	}
	for i := 0; i < 6; i++ {
		if p.items.at(i) != old.at(i) || old.at(i).IsDeleted() {
			t.Fatalf("prefix slot %d moved or retired", i)
		}
	}
	for i := 6; i < 8; i++ {
		if !old.at(i).IsDeleted() || atomic.LoadUint64(&old.at(i).reverse) != 13 {
			t.Fatalf("source slot %d is not an ordered hole", i)
		}
		if p.items.at(i+3).Key() != IntKey(i) || old.at(i).Key() != IntKey(i) {
			t.Fatalf("suffix payload lost at %d", i)
		}
	}
	if ends[0].DirectNext() != &old.at(0).ListHead ||
		old.at(5).ListHead.DirectNext() != &p.items.at(9).ListHead {
		t.Fatal("prefix is not linked to the moved suffix")
	}
}

func TestPoolSlideReusesHole(t *testing.T) {
	m := New[suffixSlideKey, int](UseEmbeddedPool[suffixSlideKey, int](true), MaxPefBucket[suffixSlideKey, int](128))
	for i := uint64(2); i <= 16; i += 2 {
		if !m.Set(suffixSlideKey(i<<40), int(i)) {
			t.Fatal("initial Set")
		}
	}
	pool := m.findBucket(9 << 40).toBase().itemPool()
	old := pool.items
	if !m.Set(suffixSlideKey(9<<40), 9) || pool.items.data != old.data {
		t.Fatal("middle Set did not slide")
	}
	length := pool.items.Len()
	// The first larger slot is a hole left by the move, before key 9.
	if !m.Set(suffixSlideKey(8<<40|1), 81) || pool.items.Len() != length {
		t.Fatal("Set did not reuse the source hole")
	}
	if item := m.bsearchBybucket(m.findBucket(8<<40|1), 8<<40|1, true); item != old.at(4) {
		t.Fatal("Set did not use the first source hole")
	}
	for i := uint64(2); i <= 16; i += 2 {
		if value, ok := m.Get(suffixSlideKey(i << 40)); !ok || value != int(i) {
			t.Fatalf("lost original key %d", i)
		}
	}
	if value, ok := m.Get(suffixSlideKey(8<<40 | 1)); !ok || value != 81 {
		t.Fatal("reused hole is not searchable")
	}
}

// purgeSlot takes the entry of slot i off the list and leaves the slot a
// deleted hole, as Delete and Purge of its key do.
func purgeSlot(t *testing.T, p *samepleItemPool[IntKey, int], i int) {
	t.Helper()
	slot := p.items.at(i)
	slot.Delete()
	if err := slot.ListHead.MarkForDelete(); err != nil {
		t.Fatal(err)
	}
	slot.ListHead.Init()
}

// checkPoolList walks the list from ends[0] and requires the keys in order,
// linked both ways.
func checkPoolList(t *testing.T, ends *[2]elist_head.ListHead, want []IntKey) {
	t.Helper()
	cur := &ends[0]
	for _, key := range want {
		next := cur.DirectNext()
		if next == &ends[1] || next.DirectPrev() != cur || entryHMapFromListHead[IntKey, int](next).Key() != key {
			t.Fatalf("broken forward/backward links at key %v", key)
		}
		cur = next
	}
	if cur.DirectNext() != &ends[1] {
		t.Fatal("broken tail links")
	}
}

// checkHoles requires the slots from first to last to be holes of reverse,
// still holding their old payload for a reader that has one of them.
func checkHoles(t *testing.T, p *samepleItemPool[IntKey, int], first, last int, reverse uint64) {
	t.Helper()
	for i := first; i <= last; i++ {
		hole := p.items.at(i)
		if !hole.IsDeleted() || atomic.LoadUint64(&hole.reverse) != reverse || hole.Key() != IntKey(i) {
			t.Fatalf("slot %d is not an ordered hole with its payload", i)
		}
	}
}

// A run of free slots after the position that holds the new entry and the
// block from the position to the first free slot takes both in one move:
// the array and its length stay, the new entry goes first, and the slots
// the block leaves stay holes ordered before it.
func TestPoolSlideIntoFreeRun(t *testing.T) {
	p, ends := makeInsertPool(8, 8)
	for _, i := range []int{5, 6, 7} {
		purgeSlot(t, p, i)
	}
	old := p.items
	e, _, _ := p.insertToPool(7, nil)
	if p.items.data != old.data || p.items.Len() != 8 {
		t.Fatal("insertion did not keep the array and its length")
	}
	if e != p.items.at(5) || atomic.LoadUint64(&e.reverse) != 7 || e.IsDeleted() {
		t.Fatal("the new entry is not at the start of the free run")
	}
	for i := 0; i < 3; i++ {
		if p.items.at(i).Key() != IntKey(i) || p.items.at(i).IsDeleted() {
			t.Fatalf("slot %d moved or retired", i)
		}
	}
	for i := 3; i < 5; i++ {
		if p.items.at(i+3).Key() != IntKey(i) || p.items.at(i+3).IsDeleted() {
			t.Fatalf("entry %d was not moved to slot %d", i, i+3)
		}
	}
	checkHoles(t, p, 3, 4, 7)
	checkPoolList(t, ends, []IntKey{0, 1, 2, 3, 4})
}

// A run of free slots that is too short goes into the block, with the
// entries after it, and the block goes to the next run that is long enough;
// here the capacity after the length completes that run.
func TestPoolSlideAbsorbsShortRun(t *testing.T) {
	p, ends := makeInsertPool(8, 12)
	for _, i := range []int{4, 6, 7} {
		purgeSlot(t, p, i)
	}
	old := p.items
	e, _, _ := p.insertToPool(5, nil)
	if p.items.data != old.data || p.items.Len() != 11 {
		t.Fatalf("insertion did not keep the array with length 11: len=%d", p.items.Len())
	}
	if e != p.items.at(6) || atomic.LoadUint64(&e.reverse) != 5 || e.IsDeleted() {
		t.Fatal("the new entry is not at the start of the free run")
	}
	// the block 2, 3, hole, 5 went to slots 7 to 10
	for i, key := range []IntKey{2, 3, 4, 5} {
		slot := p.items.at(7 + i)
		if slot.Key() != key || slot.IsDeleted() != (key == 4) {
			t.Fatalf("slot %d: key=%v deleted=%t", 7+i, slot.Key(), slot.IsDeleted())
		}
	}
	checkHoles(t, p, 2, 5, 5)
	checkPoolList(t, ends, []IntKey{0, 1, 2, 3, 5})
}

func TestPoolSlideAppendReclaimsDetachedTail(t *testing.T) {
	p, _ := makeInsertPool(8, 11)
	p.insertToPool(13, nil)
	// Removing the moved suffix also shrinks past the old source holes.
	for i := 8; i < p.items.Len(); i++ {
		p.items.at(i).Delete()
	}
	p.shrinkLen()
	if p.items.Len() != 6 {
		t.Fatalf("length after shrink = %d", p.items.Len())
	}
	slot := p.items._at(6, false, false)
	if !slot.isDetached() {
		t.Fatal("expected a detached source slot beyond the length")
	}
	e, _, _ := p.appendLast(17, nil)
	if e != slot || e.isDetached() || !e.ListHead.IsSingle() {
		t.Fatal("appended slot retains detached state or old block links")
	}
}

func TestPoolSlideRepeatedSet(t *testing.T) {
	for _, limit := range []int{16, 32, 128} {
		m := New[suffixSlideKey, int](UseEmbeddedPool[suffixSlideKey, int](true), MaxPefBucket[suffixSlideKey, int](limit))
		// Permute the low bits to insert throughout one initial bucket and split it.
		for i := 0; i < 128; i++ {
			key := suffixSlideKey(uint64((i*37)%128+1) << 40)
			if !m.Set(key, i) {
				t.Fatalf("limit=%d Set %d", limit, i)
			}
			for j := 0; j <= i; j++ {
				key := suffixSlideKey(uint64((j*37)%128+1) << 40)
				if value, ok := m.Get(key); !ok || value != j {
					t.Fatalf("limit=%d after Set %d: key %d value=%d ok=%t", limit, i, j, value, ok)
				}
			}
		}
	}
}
