package skiplistmap

import (
	"sync/atomic"
	"testing"

	"github.com/kazu/elist_head"
)

// An insert below the first entry of a pool takes the free slot before it
// as it is, and moves no entry; the free slots a split left before the
// entries are not entries of the bucket.
func TestPoolInsertTakesTheFreeSlotBefore(t *testing.T) {
	p := &samepleItemPool[IntKey, int]{reusable: true}
	// slots 0 and 1 free, entries at 2, 3, 4
	p.setItems(newPoolItems[IntKey, int, embeddedEntry[IntKey, int]](5, 8, true))
	var ends [2]elist_head.ListHead
	elist_head.InitAsEmpty(&ends[0], &ends[1])
	for i := 2; i < 5; i++ {
		e := p.ptrItems().at(i)
		e.InitEntry(IntKey(i), i)
		e.reverse = uint64(10 * i)
		e.state |= mapIsPoolItem
		if _, err := ends[1].InsertBefore(&e.ListHead); err != nil {
			t.Fatal(err)
		}
	}
	if got := p.leadingFree(); got != 2 {
		t.Fatalf("leadingFree = %d, not 2", got)
	}

	got, _, _ := p.insertToPool(15, nil, nil)
	if got != p.ptrItems().at(1) {
		t.Fatal("the entry did not go to the free slot before the first entry")
	}
	for i, want := range map[int]uint64{1: 15, 2: 20, 3: 30, 4: 40} {
		if r := atomic.LoadUint64(&p.ptrItems().at(i).reverse); r != want {
			t.Fatalf("slot %d has reverse %d, not %d: an entry moved", i, r, want)
		}
	}
	// the slot is an entry once Set fills it in
	got.state |= mapIsPoolItem
	if got := p.leadingFree(); got != 1 {
		t.Fatalf("leadingFree = %d after the insert, not 1", got)
	}
}
