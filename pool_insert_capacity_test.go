package skiplistmap

import (
	"runtime"
	"testing"
	"unsafe"
)

func TestPoolInsertDisjointCapacity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		capacity  int
		published int
		reuse     bool
	}{
		{"one_short", 12, 8, false},
		{"exact_fit", 13, 8, true},
		{"remaining_capacity", 32, 8, true},
		{"purged_exact_fit", 25, 20, true},
		{"purged_one_short", 24, 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, ends := makeInsertPool(tc.published, tc.capacity)
			old := *p.ptrItems()
			for i := 8; i < tc.published; i++ {
				p.ptrItems().at(i).Delete()
			}
			p.shrinkLen()
			if p.ptrItems().Len() != 8 {
				t.Fatal("purge did not leave eight entries")
			}
			e, _, _ := p.insertToPool(9, nil)
			insertAt, length := 4, 9
			if tc.reuse {
				insertAt, length = tc.published, tc.published+5
			}
			if e != p.ptrItems().at(insertAt) || p.ptrItems().Len() != length {
				t.Fatal("wrong insertion slot")
			}
			if tc.reuse {
				if p.ptrItems().first() != old.first() {
					t.Fatal("insertion moved the prefix")
				}
				if p.ptrItems().Cap() != tc.capacity {
					t.Fatal("new capacity extends past the old array")
				}
			} else {
				for i := 0; i < old.Cap(); i++ {
					if unsafe.Pointer(p.ptrItems().first()) == unsafe.Pointer(old._at(i, false, false)) {
						t.Fatal("insertion reused an insufficient or previously published range")
					}
				}
				if p.ptrItems().Cap() != tc.capacity {
					t.Fatal("allocation changed the existing capacity rule")
				}
			}
			e.InitEntry(IntKey(100), 100)
			e.state |= mapIsPoolItem
			if _, err := p.ptrItems().at(insertAt + 1).ListHead.InsertBefore(&e.ListHead); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < tc.published; i++ {
				if old.at(i).Key() != IntKey(i) || old.at(i).Value() != i {
					t.Fatalf("old reader lost payload at slot %d", i)
				}
			}
			starts := []int{5}
			if !tc.reuse {
				starts = append(starts, 1)
			}
			for _, first := range starts {
				cur := &old.at(first).ListHead
				for i := first + 1; i < first+3; i++ {
					cur = cur.DirectNext()
					if cur != &old.at(i).ListHead || old.at(i).Value() != i {
						t.Fatal("old traversal left its detached block")
					}
				}
				if !cur.Empty() || cur.DirectNext() != cur {
					t.Fatal("old traversal did not stop at the self-linked end")
				}
			}
			cur := &ends[0]
			for i := 0; i < 9; i++ {
				index := i
				if i >= 4 {
					index = insertAt + i - 4
				}
				next := cur.DirectNext()
				if next != &p.ptrItems().at(index).ListHead || next.DirectPrev() != cur {
					t.Fatalf("broken links at slot %d", i)
				}
				cur = next
			}
			if cur.DirectNext() != &ends[1] || ends[1].DirectPrev() != cur {
				t.Fatal("broken tail links")
			}
			runtime.GC()
			runtime.KeepAlive(old)
			runtime.KeepAlive(p)
			runtime.KeepAlive(ends)
		})
	}
}
