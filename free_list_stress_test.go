package skiplistmap

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

// Many goroutines take arrays from one free list and put them back at once,
// as the writers of the buckets of a map do: every take gets an array that
// nobody else has, every array put comes back, and the list stays whole.
func TestFreePoolsPutTakeStress(t *testing.T) {
	const workers, rounds, capacity = 16, 20000, 16
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()
	type owner struct{ holder atomic.Int32 }
	owners := map[*embeddedEntry[IntKey, int]]*owner{}
	var ownersMu sync.Mutex
	ownerOf := func(items []embeddedEntry[IntKey, int]) *owner {
		ownersMu.Lock()
		defer ownersMu.Unlock()
		first := &items[:cap(items)][0]
		o := owners[first]
		if o == nil {
			o = &owner{}
			owners[first] = o
		}
		return o
	}
	var fresh, reused, conflicts atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				items, isReused := takeItems(&f, 0, capacity, true)
				if isReused {
					reused.Add(1)
				} else {
					fresh.Add(1)
				}
				o := ownerOf(items.items)
				if !o.holder.CompareAndSwap(0, int32(w)+1) {
					conflicts.Add(1)
				}
				o.holder.Store(0)
				f.put(items.items)
			}
		}(w)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatalf("put/take did not finish; %s", describeFreeLists(&f))
	}
	if n := conflicts.Load(); n != 0 {
		t.Fatalf("%d takes got an array another goroutine had", n)
	}
	// every array is back on the list, and the list is whole
	l := &f.list
	count := 0
	prev := &l.head
	for cur := l.head.DirectNext().WithOutMark(); cur != &l.tail; cur = cur.DirectNext().WithOutMark() {
		if cur.DirectPrev().WithOutMark() != prev || count > len(owners) {
			t.Fatalf("the free list is not whole at node %d: %s", count, describeFreeLists(&f))
		}
		prev = cur
		count++
	}
	if l.tail.DirectPrev().WithOutMark() != prev {
		t.Fatalf("the tail leads back past the last node: %s", describeFreeLists(&f))
	}
	if count != len(owners) {
		view := f.view()
		pools := map[uintptr]int{}
		for cur := l.head.DirectNext().WithOutMark(); cur != &l.tail; cur = cur.DirectNext().WithOutMark() {
			pools[uintptr(unsafe.Pointer(&view.Element(cur).pool[:1][0]))]++
		}
		dup := 0
		for _, c := range pools {
			if c > 1 {
				dup++
			}
		}
		t.Fatalf("%d arrays on the list, %d were made, %d distinct pools on the list, %d pools twice, fresh %d", count, len(owners), len(pools), dup, fresh.Load())
	}
	t.Logf("fresh %d reused %d arrays %d", fresh.Load(), reused.Load(), len(owners))
}

// describeFreeLists tells the state of the free list: the links of the head
// and the tail, and the first and last nodes with their raw links, marks,
// capacity and busy flag.
func describeFreeLists[K Key[K], V any, E poolItem[K, V]](f *freePools[K, V, E]) string {
	var b strings.Builder
	view := f.view()
	l := &f.list
	node := func(h *list_head.ListHead) string {
		links := (*[2]uintptr)(unsafe.Pointer(h))
		prev, next := atomic.LoadUintptr(&links[0]), atomic.LoadUintptr(&links[1])
		s := fmt.Sprintf("%p prev %#x next %#x", h, prev, next)
		switch h {
		case &l.head:
			return s + " (head)"
		case &l.tail:
			return s + " (tail)"
		}
		n := view.Element(h)
		return s + fmt.Sprintf(" cap %d", cap(n.pool))
	}
	first, last := l.head.DirectNext().WithOutMark(), l.tail.DirectPrev().WithOutMark()
	fmt.Fprintf(&b, "list (busy %t):\n  %s\n  %s\n", l.busy.Load(), node(&l.head), node(&l.tail))
	count := 0
	for h := first; h != nil && h != &l.tail && count < 100000; h = h.DirectNext().WithOutMark() {
		if count < 3 || h.DirectNext().WithOutMark() == &l.tail {
			fmt.Fprintf(&b, "  %s\n", node(h))
		}
		count++
	}
	fmt.Fprintf(&b, "  %d nodes from the head; tail leads back to %s\n", count, node(last))
	return b.String()
}
