package skiplistmap

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
)

func TestPoolInsertBetween(t *testing.T) {
	for _, position := range []int{0, 4, 7} {
		for _, spare := range []int{0, 1, 8, 9, 24} {
			for _, state := range []string{"live", "deleted", "purged"} {
				t.Run(fmt.Sprintf("position=%d/spare=%d/state=%s", position, spare, state), func(t *testing.T) {
					deleted := state != "live"
					p, ends := makeInsertPool(8, 8+spare)
					external := NewEntry(IntKey(99), 99)
					if _, err := p.ptrItems().at(3).ListHead.InsertBefore(&external.ListHead); err != nil {
						t.Fatal(err)
					}
					if deleted {
						p.ptrItems().at(2).Delete()
					}
					if state == "purged" {
						if err := p.ptrItems().at(2).ListHead.MarkForDelete(); err != nil {
							t.Fatal(err)
						}
						p.ptrItems().at(2).ListHead.Init()
					}
					old := *p.ptrItems()
					e, _, _ := p.insertToPool(uint64(position*2+1), nil, nil)
					insertAt, length := position, 9
					if p.ptrItems().first() == old.first() {
						insertAt, length = 8, 17-position
					}
					if e != p.ptrItems().at(insertAt) || p.ptrItems().Len() != length {
						t.Fatal("wrong insertion slot")
					}
					for i := 0; i < 8; i++ {
						j := i
						if i >= position {
							j = insertAt + 1 + i - position
						}
						item := p.ptrItems().at(j)
						if item.Key() != IntKey(i) || item.Value() != i || item.IsDeleted() != (deleted && i == 2) {
							t.Fatalf("slot %d: key=%v value=%v deleted=%t", j, item.Key(), item.Value(), item.IsDeleted())
						}
					}
					want := []IntKey{0, 1, 2, 99, 3, 4, 5, 6, 7}
					if deleted && !(p.ptrItems().first() == old.first() && position > 2 && state == "deleted") {
						want = []IntKey{0, 1, 99, 3, 4, 5, 6, 7}
					}
					cur := &ends[0]
					for _, key := range want {
						next := cur.DirectNext()
						if next == &ends[1] || next.DirectPrev() != cur || entryHMapFromListHead[IntKey, int](next).Key() != key {
							t.Fatalf("broken forward/backward links at key %v", key)
						}
						cur = next
					}
					if cur.DirectNext() != &ends[1] || ends[1].DirectPrev() != cur {
						t.Fatal("broken tail links")
					}
					runtime.GC()
					runtime.KeepAlive(old)
					runtime.KeepAlive(p)
					runtime.KeepAlive(external)
				})
			}
		}
	}
}

func TestPoolInsertRepeatedMoves(t *testing.T) {
	p, ends := makeInsertPool(8, 32)
	var old []embeddedItems[IntKey, int]
	for pass := 0; pass < 12; pass++ {
		old = append(old, *p.ptrItems())
		reverse := uint64([]int{7, 3, 5, 11, 9, 13, 1, 15}[pass%8])
		e, _, _ := p.insertToPool(reverse, nil, nil)
		e.InitEntry(IntKey(100+pass), pass)
		e.state |= mapIsPoolItem
		index := 0
		for p.ptrItems().at(index) != e {
			index++
		}
		next := index + 1
		for p.ptrItems().at(next).IsIgnored() {
			next++
		}
		if _, err := p.ptrItems().at(next).ListHead.InsertBefore(&e.ListHead); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < p.ptrItems().Len(); i++ {
			item := p.ptrItems().at(i)
			origin := elist_head.FindOrigin(&item.ListHead)
			if entryHMapFromListHead[IntKey, int](origin).Key() != item.Key() {
				t.Fatalf("pass %d slot %d: origin key=%v, current key=%v", pass, i, entryHMapFromListHead[IntKey, int](origin).Key(), item.Key())
			}
		}
	}
	runtime.KeepAlive(ends)
	runtime.KeepAlive(old)
}
