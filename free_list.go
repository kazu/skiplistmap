package skiplistmap

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
)

// freePool is an array of slots that a pool of a Map let go of, on the free
// list of its capacity until a pool takes it back.
type freePool[K Key[K], V any, E poolItem[K, V]] struct {
	// pool is the whole range of slots the pool owned, with none in use
	pool []E
	list_head.ListHead
}

// freeList is the list of one size class of freePools: the arrays go in
// before the tail and out at the head.
type freeList struct {
	head, tail list_head.ListHead
	// taking is held by the take that unlinks the head: one at a time, as
	// the deletes of two nodes next to each other, which MarkForDelete of
	// lista_encabezado relinks past each other, put the first one back at
	// the head. The puts before the tail go on meanwhile.
	taking atomic.Bool
}

// freePools keeps the arrays that the pools of one Map let go of, one list
// per size class of their capacity, so that a pool that grows or rebuilds
// its array takes one back instead of allocating. An array goes to the tail
// of its list and is taken from the head, so the one that has been idle the
// longest goes out first. The lists are shared by the pools of the Map and
// changed without a lock: the writers of two buckets may take and put at
// once.
type freePools[K Key[K], V any, E poolItem[K, V]] struct {
	lists [bits.UintSize + 1]freeList
}

func (f *freePools[K, V, E]) init() {
	for i := range f.lists {
		list_head.InitAsEmpty(&f.lists[i].head, &f.lists[i].tail)
	}
}

func (f *freePools[K, V, E]) view() listaList[freePool[K, V, E]] {
	return newListaList[freePool[K, V, E]](unsafe.Offsetof(freePool[K, V, E]{}.ListHead))
}

// take returns the array at the head of the list of the size class of
// capacity, with none of its slots in use, or nil when the list is empty,
// that array is too small, or another take has the list: then the caller
// allocates. Only the head is ever taken, by one take at a time.
func (f *freePools[K, V, E]) take(capacity int) []E {
	if f == nil {
		return nil
	}
	l := &f.lists[bits.Len(uint(capacity))]
	if !l.taking.CompareAndSwap(false, true) {
		return nil
	}
	defer l.taking.Store(false)
	view := f.view()
	for {
		h := l.head.DirectNext().WithOutMark()
		if h == nil || h == &l.tail {
			return nil
		}
		n := view.Element(h)
		if cap(n.pool) < capacity {
			return nil
		}
		// the put of the array is between its two CASes: the head leads to
		// the node, the tail does not lead back yet. MarkForDelete does
		// not wait for an insert before the tail, and would take the node
		// out while the put then makes the tail lead back to it, for good
		if h.DirectNext().DirectPrev().WithOutMark() != h {
			runtime.Gosched()
			continue
		}
		for view.MarkForDelete(n) != nil {
			runtime.Gosched()
		}
		if EnableStats {
			DebugStats[CntPoolArrayReuse].Add(1)
		}
		return n.pool
	}
}

// put puts the slots of items, all of its capacity, on the free list once
// nothing holds them: the caller is the pool that owned them, under the lock
// of its bucket, and has replaced them. A slot that a node outside the array
// still leads to, as the one of an entry that Delete left on the list does
// until its unlink is repaired, keeps the array out of the free list, for
// the collector. Every slot is made as fresh as newPoolItems makes it, after
// the readers pinned on it have left; a search that still reads the array
// finds the slots fresh and starts again on the generation of its pool.
func (f *freePools[K, V, E]) put(items []E) {
	if f == nil || cap(items) == 0 {
		return
	}
	all := items[:cap(items)]
	lo := uintptr(unsafe.Pointer(&all[0]))
	hi := lo + uintptr(len(all))*unsafe.Sizeof(all[0])
	outside := func(n *elist_head.ListHead) bool {
		p := uintptr(unsafe.Pointer(n))
		return p < lo || p >= hi
	}
	for i := range all {
		h := &(*embeddedEntry[K, V])(unsafe.Pointer(&all[i])).ListHead
		if n := h.DirectNext(); n != h && outside(n) && n.DirectPrev() == h {
			return
		}
		if p := h.DirectPrev(); p != h && outside(p) && p.DirectNext() == h {
			return
		}
	}
	for i := range all {
		slot := (*embeddedEntry[K, V])(unsafe.Pointer(&all[i]))
		slot.acquireWrite()
		var zero embeddedEntry[K, V]
		slot.key, slot.value = zero.key, zero.value
		clearListLinks(&slot.ListHead)
		atomic.StoreUint64(&slot.conflict, 0)
		atomic.StoreUint64(&slot.reverse, 0)
		atomic.StoreUint64((*uint64)(&slot.state), uint64(mapIsReusable))
	}
	if EnableStats {
		DebugStats[CntPoolArrayFree].Add(1)
	}
	n := &freePool[K, V, E]{pool: all[:0]}
	// a single node, as InsertBefore of lista_encabezado requires
	list_head.InitAsEmpty(&n.ListHead, &n.ListHead)
	l := &f.lists[bits.Len(uint(cap(all)))]
	for f.view().InsertBefore(&l.tail, n) != nil {
		runtime.Gosched()
	}
}

// clearListLinks makes dst linked to itself with atomic stores, like
// copyListLinks, for a slot that a search can still read.
func clearListLinks(dst *elist_head.ListHead) {
	d := (*[2]uintptr)(unsafe.Pointer(dst))
	atomic.StoreUintptr(&d[0], 0)
	atomic.StoreUintptr(&d[1], 0)
}

// takeItems returns an array of capacity slots or more with length of them
// in use: one that a pool of the Map let go of when free has one, a new one
// otherwise. reused tells which, as a search may still read a taken array.
func takeItems[K Key[K], V any, E poolItem[K, V]](free *freePools[K, V, E], length, capacity int, reusable bool) (items itemSlice[K, V, E], reused bool) {
	if pool := free.take(capacity); pool != nil {
		return itemSlice[K, V, E]{items: pool[:length]}, true
	}
	return newPoolItems[K, V, E](length, capacity, reusable), false
}
