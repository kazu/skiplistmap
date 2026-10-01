//go:build stephook

package skiplistmap

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// StepBucketState returns the state of the bucket that a StepHook point
// passed as p.
func StepBucketState(p unsafe.Pointer) uint32 {
	return atomic.LoadUint32(&(*bucket[StringKey, any])(p).state)
}

// StepBucketLevel returns the level of the bucket that a StepHook point
// passed as p. It is negative while a split builds the bucket.
func StepBucketLevel(p unsafe.Pointer) int32 {
	return (*bucket[StringKey, any])(p).level()
}

// StepBucketDummy returns the list node of the dummy entry of the bucket that
// a StepHook point passed as p.
func StepBucketDummy(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(&(*bucket[StringKey, any])(p).dummy.ListHead)
}

// StepDirectNext returns the next of the entry list node p without skipping
// marked nodes.
func StepDirectNext(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer((*elist_head.ListHead)(p).DirectNext())
}

// StepCheckBucketsBackward walks the list of buckets backward from its tail.
// It reports two buckets that are the prev of each other, and a walk that
// does not end.
func StepCheckBucketsBackward(h *Map[StringKey, any]) error {
	const limit = 1 << 22
	n := 0
	for cur := h.tailBucket.DirectPrev(); cur != h.headBucket; cur = cur.DirectPrev() {
		if p := cur.DirectPrev(); p != cur && p.DirectPrev() == cur {
			return fmt.Errorf("bucket list backward: buckets (reverse %016x) and (reverse %016x) are the prev of each other", bucketFromListHead[StringKey, any](cur).reverse, bucketFromListHead[StringKey, any](p).reverse)
		}
		if n++; n > limit {
			return fmt.Errorf("bucket list backward does not end")
		}
	}
	return nil
}
