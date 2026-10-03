//go:build stephook

package skiplistmap

import (
	"unsafe"

	"github.com/kazu/elist_head"
)

// The helpers below are written with what the trees before the fixes also
// have, so that the J4 to J7 tests build there too.

// StepListReverse returns the reverse of the entry whose list node a StepHook
// point passed as p.
func StepListReverse(p unsafe.Pointer) uint64 {
	return mapheadFromLListHead((*elist_head.ListHead)(p)).reverse
}

// StepDirectPrev returns the prev of the entry list node p without skipping
// marked nodes.
func StepDirectPrev(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer((*elist_head.ListHead)(p).DirectPrev())
}

// StepIsMarked reports whether the entry list node p is marked for delete.
func StepIsMarked(p unsafe.Pointer) bool {
	return (*elist_head.ListHead)(p).IsMarked()
}

// StepBucketsForward returns the reverses of the buckets that a walk from the
// head of the list of buckets reaches, in the way addBucket walks. It stops
// after max buckets.
func StepBucketsForward(h *Map[StringKey, any],

	max int) (reverses []uint64) {
	for cur := h.headBucket.Prev().Next(); !cur.Empty() && len(reverses) < max; cur = cur.Next() {
		reverses = append(reverses, bucketFromListHead[StringKey, any](cur).reverse)
	}
	return reverses
}
