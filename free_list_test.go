package skiplistmap

import (
	"testing"

	"github.com/kazu/elist_head"
)

// An array that a pool let go of comes back from the free list with every
// slot as fresh as a new one, once; it does not come back for a larger
// capacity, and a slot that a node outside the array still leads to keeps
// the array out of the list.
func TestFreePoolsTakeBackAnArray(t *testing.T) {
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()

	items, reused := takeItems(&f, 0, 8, true)
	if reused || items.Cap() != 8 {
		t.Fatalf("takeItems from an empty list = reused %v cap %d", reused, items.Cap())
	}
	for i := 0; i < 8; i++ {
		slot := items._at(i, false, false)
		slot.initializePayload(IntKey(i), i)
		slot.reverse, slot.conflict = uint64(i)+1, 7
	}
	f.put(items.items)
	again, reused := takeItems(&f, 3, 8, true)
	if !reused || again.first() != items.first() || again.Cap() != 8 || again.Len() != 3 {
		t.Fatalf("the array that was let go of did not come back: reused %v len %d cap %d", reused, again.Len(), again.Cap())
	}
	var fresh embeddedEntry[IntKey, int]
	fresh.state = mapIsReusable
	for i := 0; i < 8; i++ {
		if slot := again._at(i, false, false); *slot != fresh {
			t.Fatalf("slot %d is not fresh: %+v", i, *slot)
		}
	}
	if f.take(8) != nil {
		t.Fatal("the array came back twice")
	}
	// an array smaller than the capacity asked for leaves the list, and the
	// one behind it comes back
	f.put(again.items)
	larger, _ := takeItems(&f, 0, 16, true)
	f.put(larger.items)
	if got := f.take(16); got == nil || &got[:1][0] != larger.first() {
		t.Fatal("the larger array behind a smaller one did not come back")
	}
	if f.take(8) != nil {
		t.Fatal("an array smaller than the capacity asked for stayed on the list")
	}

	// a slot that the list still leads to
	kept, _ := takeItems(&f, 2, 8, true)
	var ends [2]elist_head.ListHead
	elist_head.InitAsEmpty(&ends[0], &ends[1])
	slot := kept._at(0, false, false)
	slot.ListHead.Init()
	if _, err := ends[1].InsertBefore(&slot.ListHead); err != nil {
		t.Fatal(err)
	}
	f.put(kept.items)
	if f.take(8) != nil {
		t.Fatal("an array that a node leads into went to the free list")
	}
}
