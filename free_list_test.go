package skiplistmap

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kazu/elist_head"
)

// A take from an empty list cuts a chunk of arrays, keeps one and puts the
// rest on the list, so that the takes after it find the list full; the
// chunks grow as the arrays cut so far, as a slice grows.
func TestFreePoolsCutArraysInChunks(t *testing.T) {
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()
	f.capacity = 8
	drain := func() (n int) {
		for f.take(8) != nil {
			n++
		}
		return n
	}
	first, reused := takeItems(&f, 2, 8, true)
	if reused || first.Len() != 2 || first.Cap() != 8 {
		t.Fatalf("the first take: reused %v len %d cap %d", reused, first.Len(), first.Cap())
	}
	if n := drain(); n != minChunkArrays-1 {
		t.Fatalf("%d arrays on the list after the first cut, not %d", n, minChunkArrays-1)
	}
	for i, want := range []int{minChunkArrays - 1, 2*minChunkArrays - 1} {
		if _, reused := takeItems(&f, 0, 8, true); reused {
			t.Fatal("a take from the empty list came back as reused")
		}
		if n := drain(); n != want {
			t.Fatalf("%d arrays on the list after cut %d, not %d", n, i+2, want)
		}
	}
	if cut := f.cutArrays.Load(); cut != 4*minChunkArrays {
		t.Fatalf("%d arrays cut, not %d", cut, 4*minChunkArrays)
	}
	// the arrays of a chunk are apart: a put of one leaves the others alone
	a, _ := takeItems(&f, 8, 8, true)
	b, _ := takeItems(&f, 8, 8, true)
	a._at(0, false, false).initializePayload(IntKey(1), 1)
	f.put(b.items)
	if a._at(0, false, false).value != 1 {
		t.Fatal("the put of one array of a chunk touched another")
	}
	// an array smaller than the capacity the takes ask for stays off the
	// list, where it would turn every take away
	drain()
	small := newPoolItems[IntKey, int, embeddedEntry[IntKey, int]](0, 4, true)
	f.put(small.items)
	if f.take(4) != nil {
		t.Fatal("an array smaller than the capacity went on the list")
	}
}

// A bucket that splits gives the child an array of the capacity a pool starts
// with and keeps its own, so that every array a pool lets go of or asks for
// is of that one capacity, and the free list hands them on.
func TestFreePoolsSplitKeepsOneCapacity(t *testing.T) {
	// the map is built before the stats are on, which dump the first buckets
	h := New[StringKey, int](UseEmbeddedPool[StringKey, int](true), MaxPefBucket[StringKey, int](16), BucketMode[StringKey, int](CombineSearch3))
	previous := EnableStats
	defer func() { EnableStats = previous }()
	EnableStats = true
	ResetStats()
	ResetCapStats()
	for i := 0; i < 20000; i++ {
		h.Set(StringKey(fmt.Sprintf("k%d", i)), i)
	}
	want := h.poolInitCap()
	stats := CapStats()
	if DebugStats[CntPoolArrayFree].Load() == 0 || strings.Count(stats, "put") == 0 {
		t.Fatalf("no array went through the free list:\n%s", stats)
	}
	for _, line := range strings.Split(stats, "\n") {
		var kind string
		var have, capacity, count int
		if _, err := fmt.Sscanf(line, "%s have=%d want=%d count=%d", &kind, &have, &capacity, &count); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		// a pool that cannot split grows past the capacity, asks the list
		// for more, which it does not hold, and lets its larger array go
		// later; what this is about is that a take finds the capacity it
		// asks for
		if kind != "reuse" {
			continue
		}
		if capacity != want || have < want {
			t.Fatalf("a take asked for another capacity than %d, or found less: %s", want, line)
		}
	}
}

// A search that read the array of a pool before the pool let it go reads it
// still, past the length of the pool that took it back: a slide of that pool
// into the slots past its length, which no search of its own reads, is
// written with atomic stores. Run with -race: the search and the slide have
// no order between them.
func TestFreePoolsSlideOnTakenArrayIsAtomic(t *testing.T) {
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()
	first, ends := makeInsertPool(8, 16)
	snapshot := first.ptrItems().items

	ready, done, left := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(left)
		close(ready)
		for {
			select {
			case <-done:
				return
			default:
			}
			// the slots past the length of the pool that takes the array
			for i := 4; i < len(snapshot); i++ {
				atomic.LoadUint64(&snapshot[i].reverse)
			}
		}
	}()
	<-ready

	// the pool lets the array go: nothing outside it leads in any more
	for i := 0; i < 8; i++ {
		first.ptrItems().at(i).ListHead.Init()
	}
	elist_head.InitAsEmpty(&ends[0], &ends[1])
	f.put(first.ptrItems().items)

	// another pool takes it with four entries, and slides three of them
	// into the slots past its length
	taken, reused := takeItems(&f, 4, 16, true)
	if !reused {
		t.Fatal("the array did not come back")
	}
	second := &samepleItemPool[IntKey, int]{reusable: true}
	second.setItems(taken)
	var otherEnds [2]elist_head.ListHead
	elist_head.InitAsEmpty(&otherEnds[0], &otherEnds[1])
	for i := 0; i < 4; i++ {
		e := second.ptrItems().at(i)
		e.InitEntry(IntKey(i), i)
		e.reverse = uint64(2 * (i + 1))
		e.state |= mapIsPoolItem
		if _, err := otherEnds[1].InsertBefore(&e.ListHead); err != nil {
			t.Fatal(err)
		}
	}
	second.slideBlockToFreeRun(3, 1, 4, 4, 4)
	close(done)
	<-left
}

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
	// an array smaller than the capacity asked for stays on the list, for
	// a take of its capacity
	f.put(again.items)
	if f.take(16) != nil {
		t.Fatal("an array smaller than the capacity asked for came back")
	}
	if got := f.take(8); got == nil || &got[:1][0] != again.first() {
		t.Fatal("the array did not stay for a take of its capacity")
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
