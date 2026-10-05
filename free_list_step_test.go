//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

// describeFreeLists tells the state of every free list that holds a node:
// the links of the head and the tail, and the first and last nodes with
// their raw links, marks, capacity and taking flag.
func describeFreeLists[K Key[K], V any, E poolItem[K, V]](f *freePools[K, V, E]) string {
	var b strings.Builder
	view := f.view()
	node := func(h *list_head.ListHead, l *freeList) string {
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
	for i := range f.lists {
		l := &f.lists[i]
		first, last := l.head.DirectNext().WithOutMark(), l.tail.DirectPrev().WithOutMark()
		if first == &l.tail && last == &l.head {
			continue
		}
		fmt.Fprintf(&b, "list %d (taking %t):\n  %s\n  %s\n", i, l.taking.Load(), node(&l.head, l), node(&l.tail, l))
		count := 0
		for h := first; h != nil && h != &l.tail && count < 100000; h = h.DirectNext().WithOutMark() {
			if count < 3 || h.DirectNext().WithOutMark() == &l.tail {
				fmt.Fprintf(&b, "  %s\n", node(h, l))
			}
			count++
		}
		fmt.Fprintf(&b, "  %d nodes from the head; tail leads back to %s\n", count, node(last, l))
	}
	return b.String()
}

// Two pools that take an array from the free list at once get two arrays, or
// one of them gets none: an array never goes to both, and the one that gets
// none does not wait. One take is stopped in its MarkForDelete before it
// locks, where the other runs to the end.
func TestFreePoolsTakeHandsOutAnArrayOnce(t *testing.T) {
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()
	items, _ := takeItems(&f, 0, 8, true)
	f.put(items.items)
	node := f.lists[bits.Len(8)].head.Next()

	reached, resume := make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	list_head.SetStepHook(func(point string, n, _, _ *list_head.ListHead) {
		if point == "del.purgeable" && n == node && stopped.CompareAndSwap(false, true) {
			close(reached)
			<-resume
		}
	})
	defer list_head.SetStepHook(nil)

	first := make(chan []embeddedEntry[IntKey, int], 1)
	go func() { first <- f.take(8) }()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("the take did not reach MarkForDelete of the node")
	}
	second := f.take(8)
	close(resume)
	var got []embeddedEntry[IntKey, int]
	select {
	case got = <-first:
	case <-time.After(time.Second):
		t.Fatal("the stopped take did not finish")
	}
	if got == nil {
		t.Fatal("the take that unlinks the node did not get the array")
	}
	if second != nil {
		t.Fatal("both takes got the array")
	}
}

// An array whose put has linked it from the head of the list but not yet
// from the tail is not taken: a take of it would leave the tail leading back
// to an array that a pool has, and every later put, which inserts before the
// tail, would fail for ever. The put is stopped between its two CASes, where
// the take runs; both finish, and the list is whole.
func TestFreePoolsTakeWaitsForAHalfInsertedArray(t *testing.T) {
	var f freePools[IntKey, int, embeddedEntry[IntKey, int]]
	f.init()
	items, _ := takeItems(&f, 0, 8, true)
	l := &f.lists[bits.Len(8)]

	reached, resume := make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	list_head.SetStepHook(func(point string, _, _, at *list_head.ListHead) {
		if point == "add.cas2" && at == &l.tail && stopped.CompareAndSwap(false, true) {
			close(reached)
			<-resume
		}
	})
	defer list_head.SetStepHook(nil)

	putDone := make(chan struct{})
	go func() { f.put(items.items); close(putDone) }()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("put did not link the array from the head")
	}
	taken := make(chan []embeddedEntry[IntKey, int], 1)
	go func() { taken <- f.take(8) }()
	time.Sleep(10 * time.Millisecond)
	close(resume)
	var got []embeddedEntry[IntKey, int]
	select {
	case got = <-taken:
	case <-time.After(time.Second):
		t.Fatal("take did not finish")
	}
	select {
	case <-putDone:
	case <-time.After(time.Second):
		t.Fatal("put did not finish")
	}
	first, last := l.head.DirectNext(), l.tail.DirectPrev()
	switch {
	case got != nil && (first != &l.tail || last != &l.head):
		t.Fatalf("the array was taken but the list still leads to a node: head -> %p, tail -> %p", first, last)
	case got == nil && (first == &l.tail || last == &l.head || first != last):
		t.Fatalf("the array was not taken and the list does not hold it alone: head -> %p, tail -> %p", first, last)
	}
}

// With the free list in use, concurrent inserts of new keys into a map of
// embedded pools finish, and the free lists stay whole: the head of each
// leads to the node the tail leads back to.
func TestFreePoolsConcurrentInsertsFinish(t *testing.T) {
	preload, workers, perWork := 20000, 16, 2000
	for _, p := range []struct {
		name string
		to   *int
	}{{"SKIPLISTMAP_FREE_PRELOAD", &preload}, {"SKIPLISTMAP_FREE_WORKERS", &workers}, {"SKIPLISTMAP_FREE_PERWORK", &perWork}} {
		if v, err := strconv.Atoi(os.Getenv(p.name)); err == nil && v > 0 {
			*p.to = v
		}
	}
	// the map of the insert benchmark: the pools start at the size of a bucket
	m := New[StringKey, any](UseEmbeddedPool[StringKey, any](true), MaxPefBucket[StringKey, any](16), MinCapItems[StringKey, any](16), BucketMode[StringKey, any](CombineSearch3))
	for i := 0; i < preload; i++ {
		if !m.Set(StringKey(fmt.Sprintf("%d", i)), i) {
			t.Fatalf("preload Set %d failed", i)
		}
	}
	// with overlap, the workers walk the same keys from different starts,
	// as the workers of the insert benchmark do, so most of their Sets
	// find the key and replace its entry
	overlap := os.Getenv("SKIPLISTMAP_FREE_OVERLAP") != ""
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWork; i++ {
				k := w*perWork + i
				if overlap {
					k = w + i
				}
				m.Set(StringKey(fmt.Sprintf("xx%dxx", k)), w)
			}
		}(w)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatalf("concurrent inserts did not finish; free lists:\n%s", describeFreeLists(&m.free))
	}
	for i := range m.free.lists {
		l := &m.free.lists[i]
		first, last := l.head.DirectNext(), l.tail.DirectPrev()
		if (first == &l.tail) != (last == &l.head) {
			t.Fatalf("free list %d is not whole: head -> %p, tail -> %p\n%s", i, first, last, describeFreeLists(&m.free))
		}
	}
	want := preload + workers*perWork
	if overlap {
		want = preload + workers - 1 + perWork
	}
	if got := m.Len(); got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
}
