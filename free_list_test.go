package skiplistmap

import (
	"testing"

	"github.com/kazu/elist_head"
)

// An array that a pool let go of comes back from the free list of its size
// class with every slot as fresh as a new one, once; an array of another
// size class does not come back for it, and a slot that a node outside the
// array still leads to keeps the array out of the list.
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
	if f.take(16) != nil {
		t.Fatal("an array of another size class came back")
	}
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
