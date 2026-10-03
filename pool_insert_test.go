package skiplistmap

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
)

func TestPoolInsertBetween(t *testing.T) {
	for _, position := range []int{0, 4, 7} {
		for _, spare := range []int{0, 1} {
			for _, state := range []string{"live", "deleted", "purged"} {
				t.Run(fmt.Sprintf("position=%d/spare=%d/state=%s", position, spare, state), func(t *testing.T) {
					deleted := state != "live"
					p, ends := makeInsertPool(8, 8+spare)
					external := NewEntry(IntKey(99), 99)
					if _, err := p.items.at(3).ListHead.InsertBefore(&external.ListHead); err != nil {
						t.Fatal(err)
					}
					if deleted {
						p.items.at(2).Delete()
					}
					if state == "purged" {
						if err := p.items.at(2).ListHead.MarkForDelete(); err != nil {
							t.Fatal(err)
						}
						p.items.at(2).ListHead.Init()
					}
					old := p.items
					e, _, _ := p.insertToPool(uint64(position*2+1), nil)
					if e != p.items.at(position) || p.items.Len() != 9 {
						t.Fatal("wrong insertion slot")
					}
					for i := 0; i < 8; i++ {
						j := i
						if i >= position {
							j++
						}
						item := p.items.at(j)
						if item.Key() != IntKey(i) || item.Value() != i || item.IsDeleted() != (deleted && i == 2) {
							t.Fatalf("slot %d: key=%v value=%v deleted=%t", j, item.Key(), item.Value(), item.IsDeleted())
						}
					}
					want := []IntKey{0, 1, 2, 99, 3, 4, 5, 6, 7}
					if deleted {
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
	var old []itemSlice[IntKey, int]
	for pass := 0; pass < 12; pass++ {
		old = append(old, p.items)
		reverse := uint64([]int{7, 3, 5, 11, 9, 13, 1, 15}[pass%8])
		e, _, _ := p.insertToPool(reverse, nil)
		e.InitEntry(IntKey(100+pass), pass)
		e.state |= mapIsPoolItem
		index := 0
		for p.items.at(index) != e {
			index++
		}
		if _, err := p.items.at(index + 1).ListHead.InsertBefore(&e.ListHead); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < p.items.Len(); i++ {
			item := p.items.at(i)
			origin := elist_head.FindOrigin(&item.ListHead)
			if entryHMapFromListHead[IntKey, int](origin).Key() != item.Key() {
				t.Fatalf("pass %d slot %d: origin key=%v, current key=%v", pass, i, entryHMapFromListHead[IntKey, int](origin).Key(), item.Key())
			}
		}
	}
	runtime.KeepAlive(ends)
	runtime.KeepAlive(old)
}
