package skiplistmap

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/elist_head"
)

func makeInsertPool(n, capacity int) (*samepleItemPool[IntKey, int], *[2]elist_head.ListHead) {
	p := &samepleItemPool[IntKey, int]{items: newPoolItems[IntKey, int](n, capacity, true), reusable: true}
	ends := new([2]elist_head.ListHead)
	elist_head.InitAsEmpty(&ends[0], &ends[1])
	for i := 0; i < n; i++ {
		e := p.items.at(i)
		e.InitEntry(IntKey(i), i)
		e.reverse = uint64(2 * (i + 1))
		e.state |= mapIsPoolItem
		if _, err := ends[1].InsertBefore(&e.ListHead); err != nil {
			panic(err)
		}
	}
	return p, ends
}

// BenchmarkPoolInsertBetween times only placing a slot between existing
// entries. Pool construction is outside the timer; spare capacity excludes
// expand, and the full-capacity case exercises the same insertion operation.
func BenchmarkPoolInsertBetween(b *testing.B) {
	benchmarkPoolInsertBetween(b, false)
}

// BenchmarkPoolEntryInsertBetween also initializes the new payload and links
// the entry between its neighbors, with construction of the old pool excluded.
func BenchmarkPoolEntryInsertBetween(b *testing.B) {
	benchmarkPoolInsertBetween(b, true)
}

func benchmarkPoolInsertBetween(b *testing.B, linkEntry bool) {
	for _, n := range []int{8, 32, 128, 512} {
		for _, spare := range []int{0, 1, n + 1, 4*n + 4} {
			b.Run(fmt.Sprintf("entries=%d/spare=%d", n, spare), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					p, ends := makeInsertPool(n, n+spare)
					b.StartTimer()
					e, _, _ := p.insertToPool(uint64(n+1), nil)
					if linkEntry {
						e.InitEntry(IntKey(n), n)
						e.reverse = uint64(n + 1)
						e.state |= mapIsPoolItem
						if _, err := p.items.at(n/2 + 1).ListHead.InsertBefore(&e.ListHead); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if e == nil || p.items.Len() != n+1 {
						b.Fatal("insertion did not allocate one slot")
					}
					if linkEntry && (e.ListHead.DirectPrev() != &p.items.at(n/2-1).ListHead || e.ListHead.DirectNext() != &p.items.at(n/2+1).ListHead) {
						b.Fatal("entry is not linked between its neighbors")
					}
					runtime.KeepAlive(ends)
					runtime.KeepAlive(p)
				}
			})
		}
	}
}
