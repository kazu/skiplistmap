//go:build stephook

package skiplistmap

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// StepHook is called at named points of an insertion when the package is
// built with the stephook tag, so that a test can stop a goroutine there and
// replay a concurrent interleaving one step at a time.
//
// Points and arguments:
//   - "add2.found" (item, pos): add2 found the position to insert item before.
//   - "makeBucket.claimed" (bucket, item): bucketFromPool returned bucket for the split started by item.
//   - "bucketFromPool.lenStored" (bucket, nil): the length of the new downLevels of bucket is stored.
//   - "insertBucket.begin" (bucket, nil): before the dummy of bucket is initialized.
//   - "insertBucket.dummyLinked" (bucket, nil): the dummy of bucket is linked; before bucket is linked.
type StepHook func(point string, a, b unsafe.Pointer)

const stepEnabled = true

var stepHook atomic.Pointer[StepHook]

// SetStepHook installs fn, or removes the hook when fn is nil.
func SetStepHook(fn StepHook) {
	if fn == nil {
		stepHook.Store(nil)
		return
	}
	stepHook.Store(&fn)
}

func stepAt(point string, a, b unsafe.Pointer) {
	if fn := stepHook.Load(); fn != nil {
		(*fn)(point, a, b)
	}
}

// StepBucketReverse returns the reverse of the bucket that a StepHook point
// passed as p.
func StepBucketReverse(p unsafe.Pointer) uint64 {
	return (*bucket)(p).reverse
}

// StepCheckLists walks the list of entries forward from its head and the list
// of buckets forward from its head. It reports the first break: a node out of
// order, a list that stops at a self-linked node before its tail, or a list
// that does not end.
func StepCheckLists(h *Map) error {
	const limit = 1 << 22
	n := 0
	var prev uint64
	for cur := h.head.DirectNext(); cur != h.tail; cur = cur.DirectNext() {
		r := EmptyMapHead.FromListHead(cur).reverse
		if cur.DirectNext() == cur {
			return fmt.Errorf("entry list stops at a self-linked node (reverse %016x) before its tail", r)
		}
		if r < prev {
			return fmt.Errorf("entry list out of order: %016x after %016x", r, prev)
		}
		prev = r
		if n++; n > limit {
			return fmt.Errorf("entry list does not end")
		}
	}
	n = 0
	first := true
	for cur := h.headBucket.DirectNext(); cur != h.tailBucket; cur = cur.DirectNext() {
		r := bucketFromListHead(cur).reverse
		if cur.DirectNext() == cur {
			return fmt.Errorf("bucket list stops at a self-linked bucket (reverse %016x) before its tail", r)
		}
		if !first && r > prev {
			return fmt.Errorf("bucket list out of order: %016x after %016x", r, prev)
		}
		prev, first = r, false
		if n++; n > limit {
			return fmt.Errorf("bucket list does not end")
		}
	}
	return nil
}
