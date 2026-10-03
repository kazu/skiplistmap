//go:build stephook

package skiplistmap

import (
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

// The helpers below are written with what the trees before the fixes also
// have, so that the J24 tests build there too.

// StepM24LockBuckets returns the bucket that a lookup of reverse finds, whose
// muPool Purge locks, and the bucket that owns its item pool (toBase), whose
// muPool Set of a new key locks.
func StepM24LockBuckets(h *Map[StringKey, any],

	reverse uint64) (found, base unsafe.Pointer) {
	b := h.findBucket(reverse)
	return unsafe.Pointer(b), unsafe.Pointer(b.toBase())
}

// StepM24PoolList returns the head and the terminating tail of the pool list
// that Pool.Get uses for reverse, and the pools linked between them, walking
// the links without the mark bit.
func StepM24PoolList(h *Map[StringKey, any],

	reverse uint64) (head, tail unsafe.Pointer, pools []unsafe.Pointer) {
	idx := reverse >> (4 * 15) % cntOfPoolMgr
	cur := &h.pooler.itemPool[idx].ListHead
	head = unsafe.Pointer(cur)
	for cur = cur.DirectNext().WithOutMark(); cur.DirectNext().WithOutMark() != cur; cur = cur.DirectNext().WithOutMark() {
		pools = append(pools, unsafe.Pointer(samepleItemPoolFromListHead[StringKey, any](cur)))
	}
	return head, unsafe.Pointer(cur), pools
}

// StepM24PoolLinks returns the list node of the pool p, which a StepHook
// point passed, and its prev and next without the mark bit.
func StepM24PoolLinks(p unsafe.Pointer) (node, prev, next unsafe.Pointer) {
	l := &(*samepleItemPool[StringKey, any])(p).ListHead
	return unsafe.Pointer(l), unsafe.Pointer(l.DirectPrev().WithOutMark()), unsafe.Pointer(l.DirectNext().WithOutMark())
}

// StepM24ListaLinks returns the prev and the next of the lista node p
// without the mark bit.
func StepM24ListaLinks(p unsafe.Pointer) (prev, next unsafe.Pointer) {
	l := (*list_head.ListHead)(p)
	return unsafe.Pointer(l.DirectPrev().WithOutMark()), unsafe.Pointer(l.DirectNext().WithOutMark())
}

// StepM24PoolIndex returns the index of the item p in the item pool of the
// bucket base, and the length of the pool, or -1 when p is not in it.
func StepM24PoolIndex(base unsafe.Pointer, p unsafe.Pointer) (idx, n int) {
	items := (*bucket[StringKey, any])(base).itemPool().ptrItems()
	n = items.Len()
	for i := 0; i < n; i++ {
		if unsafe.Pointer(&items.at(i).ListHead) == p {
			return i, n
		}
	}
	return -1, n
}
