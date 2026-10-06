package skiplistmap

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
)

// capStats counts, under EnableStats, the capacities that go through the
// free list: what put puts, and what take asks for against what it finds.
var capStats sync.Map

func countCap(kind string, have, want int) {
	key := fmt.Sprintf("%-5s have=%-4d want=%-4d", kind, have, want)
	v, _ := capStats.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// CapStats returns the counts of the capacities that went through the free
// list since ResetCapStats, under EnableStats, one line per kind and
// capacity: put (have = the capacity put), reuse / drop (have = the head,
// want = the capacity asked), empty (want = the capacity asked of an empty
// list). The lines are sorted.
func CapStats() string {
	var lines []string
	capStats.Range(func(k, v any) bool {
		lines = append(lines, fmt.Sprintf("%s count=%d", k.(string), v.(*atomic.Int64).Load()))
		return true
	})
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// ResetCapStats clears the counts that CapStats returns.
func ResetCapStats() {
	capStats.Range(func(k, _ any) bool { capStats.Delete(k); return true })
}

// freePool is an array of slots that a pool of a Map let go of, on the free
// list until a pool takes it back.
type freePool[K Key[K], V any, E poolItem[K, V]] struct {
	// pool is the whole range of slots the pool owned, with none in use
	pool []E
	list_head.ListHead
}

// freeList is the list of freePools: the arrays go in before the tail and
// out at the head.
type freeList struct {
	head, tail list_head.ListHead
	// busy is held by the take or the put that changes the list: one at a
	// time, as a delete of the only node while an insert goes in before the
	// tail leaves the node on the list, and the puts and takes are short
	busy atomic.Bool
}

// claim takes the list for one take or put, waiting for the one that has it.
func (l *freeList) claim() {
	for !l.busy.CompareAndSwap(false, true) {
		runtime.Gosched()
	}
}

func (l *freeList) release() {
	l.busy.Store(false)
}

// freePools keeps the arrays that the pools of one Map let go of, on one
// list, so that a pool that grows or rebuilds its array takes one back
// instead of allocating. An array goes to the tail of the list and is taken
// from the head, so the one that has been idle the longest goes out first.
// The pools start at one capacity and grow by doubling, so an array at the
// head smaller than a take asks for is from an earlier growth, and leaves.
// The list is shared by the pools of the Map: the writers of two buckets
// may take and put at once.
type freePools[K Key[K], V any, E poolItem[K, V]] struct {
	list freeList
}

func (f *freePools[K, V, E]) init() {
	list_head.InitAsEmpty(&f.list.head, &f.list.tail)
}

func (f *freePools[K, V, E]) view() listaList[freePool[K, V, E]] {
	return newListaList[freePool[K, V, E]](unsafe.Offsetof(freePool[K, V, E]{}.ListHead))
}

// take returns the first array from the head of the list of at least
// capacity, with none of its slots in use, or nil when there is none: then
// the caller allocates. The smaller arrays before it leave the list for the
// collector. No put or other take changes the list meanwhile.
func (f *freePools[K, V, E]) take(capacity int) []E {
	if f == nil {
		return nil
	}
	l := &f.list
	l.claim()
	defer l.release()
	view := f.view()
	for {
		h := l.head.DirectNext().WithOutMark()
		if h == nil || h == &l.tail {
			if EnableStats {
				countCap("empty", 0, capacity)
			}
			return nil
		}
		n := view.Element(h)
		for view.MarkForDelete(n) != nil {
			runtime.Gosched()
		}
		if EnableStats {
			if cap(n.pool) < capacity {
				countCap("drop", cap(n.pool), capacity)
			} else {
				countCap("reuse", cap(n.pool), capacity)
			}
		}
		if cap(n.pool) < capacity {
			// an array smaller than what is asked for now is from an earlier
			// growth of a pool, and the pools only grow: it goes to the
			// collector, so that it does not keep the head from the arrays
			// behind it
			if EnableStats {
				DebugStats[CntPoolArrayDrop].Add(1)
			}
			continue
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
		countCap("put", cap(all), 0)
	}
	n := &freePool[K, V, E]{pool: all[:0]}
	// a single node, as InsertBefore of lista_encabezado requires
	list_head.InitAsEmpty(&n.ListHead, &n.ListHead)
	l := &f.list
	l.claim()
	defer l.release()
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
