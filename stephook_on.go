//go:build stephook

package skiplistmap

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// StepHook is called at named points of an insertion when the package is
// built with the stephook tag, so that a test can stop a goroutine there and
// replay a concurrent interleaving one step at a time.
//
// Points and arguments:
//   - "add2.found" (item, pos): add2 found the position to insert item before.
//   - "makeBucket.begin" (item, nil): _set starts to split the bucket of item.
//   - "makeBucket.claimed" (bucket, item): bucketFromPool returned bucket for the split started by item.
//   - "bucketFromPool.lenStored" (bucket, nil): the length of the new downLevels of bucket is stored.
//   - "insertBucket.begin" (bucket, nil): before the dummy of bucket is initialized.
//   - "insertBucket.dummyLinked" (bucket, nil): the dummy of bucket is linked; before bucket is linked.
//   - "pool.expand.copied" (pool, new pool): _expand copied the items of pool into new pool; before it repairs the links.
//   - "pool.lastSlot" (pool, nil): Get read the length of pool and is taking its last item; before pool.mu is locked.
//   - "pool.get.pool" (node, nil): Pool.Get took node, the Next of the head of a pool list, as the list node of the pool to get an item from; before it calls Get of that pool.
//   - "pool.get.expand" (pool, nil): Get of pool found pool full; before it reads the next of pool to look for a next pool.
//   - "pool.get.nextPool" (pool, node): Get of pool found pool full and took node, the next of pool, as the list node of a next pool; before it calls Get of that pool.
//   - "pool.expand.begin" (pool, nil): _expand of pool starts; before pool.mu is locked.
//   - "pool.expand.marked" (pool, nil): _expand marked pool and unlinked it from the pool list; before it copies the items of pool.
//   - "pool.expand.beforeSafety" (pool, new pool): _expand linked new pool into the pool list; before it asks IsSafety of pool whether to run Init on it.
//   - "loadItem.found" (item, bucket): loadItem found item in bucket; before bucket.muPool is locked.
//   - "set.updateFound" (item, bucket): Set of a key present in a map with the embedded pool found item in bucket; before it tries to lock bucket.muPool.
//   - "set.updateLocked" (item, bucket): Set of a key present locked bucket.muPool; before it stores the value into item.
//   - "purge.beforeInit" (item, nil): purgeInEmbedded returned from MarkForDelete of item; before it runs Init on item.
//   - "makeBucket2.got" (new bucket, bucket): makeBucket2 of bucket got new bucket from bucketFromPoolEmbedded; before it runs Init on new bucket.
//   - "makeBucket2.recurse" (bucket, new bucket): makeBucket2 of bucket published new bucket; before it checks whether new bucket is over the limit and, holding the muPool of new bucket, splits it.
//   - "bucketFromPoolEmbedded.claim" (down, bucket): down, an element of the downLevels of bucket, has level 0; before its level is set.
//   - "bucketFromPoolEmbedded.claimed" (down, nil): the level and the reverse of down are set; before it is returned.
//   - "appendLast.claimed" (item, nil): appendLast raised the length of the pool over item, its last slot; before it clears the state of item.
//   - "insertToPool.publish" (pool, nil): insertToPool linked the new array of pool into the list; before it stores the array into pool.
//   - "bsearch.searched" (bucket, nil): bsearchBybucket found the index of the first item not below the key in the pool of bucket; before it reads the reverse at that index.
//   - "makeBucket.pairFound" (bucket, next bucket): makeBucket found the bucket of the entry before the split point and the next bucket above it, and computed the reverse of the new bucket from them; before it claims the new bucket from the pool.
//   - "bucketFromPool.levelFound" (bucket, pos): bucketFromPool walked the level list for the first downLevels of bucket and found pos to insert it before; before it inserts.
//   - "bucketFromPoolEmbedded.levelFound" (bucket, pos): the same as "bucketFromPool.levelFound", in bucketFromPoolEmbedded.
//   - "makeBucket.levelFound" (bucket, pos): findNextLevelBucket returned pos, the LevelHead to insert bucket around in its level list; before makeBucket inserts.
//   - "makeBucket.pairWalk" (bucket, bucket found so far): makeBucket walks the list of buckets backward to the bucket above the split point; called at the head of each step with the bucket of the step and the highest bucket not above the entry so far.
//   - "makeBucket.beforeInit" (bucket, nil): makeBucket found the dummy of bucket empty; before it runs Init on bucket and on its LevelHead.
//   - "makeBucket.added" (bucket, nil): makeBucket linked bucket and its dummy by addBucket; before it turns a negative level of bucket positive.
//   - "add2.bucketInsert" (item, right):add2 found no position for item and took right, from the bucket given to it, as the entry to link item before; before inserBeforeWithCheck(right, item).
//   - "add2.tailInsert" (item, right): add2 found no position for item and was given no bucket with an entry, and took right, the node before the tail, as the entry to link item before; before inserBeforeWithCheck(right, item).
//   - "set.beforeInit" (item, start): _set chose start, the node to find the position of item from; before it runs Init on the list node of item.
//   - "set.waitLinked" (item, nil): another store of item is linking it; before _set waits for that store.
//   - "set.expandOverlapped" (item, nil): Set or StoreItem linked item while an expand of an item pool ran; before it looks the key of item up again.
//   - "find.begin" (start, nil): find starts to walk the list of entries from start; before it reads start.
//   - "bsearch.begin" (bucket, nil): bsearchBybucket was called with bucket; before it reads the item pool of bucket and its length.
//   - "set.newKeyLock" (bucket, mutex): Set of a key not present in a map with the embedded pool found bucket and is about to lock mutex, the muPool that guards the insertion; before it locks mutex.
//   - "makeBucket2.added" (new bucket, bucket): makeBucket2 of bucket returned from addBucket of new bucket; before it turns a negative level of new bucket positive.
//   - "findNextLevelBucket.front" (front, nil): findNextLevelBucket set the traversal mode of lista to WaitNoMark and Front of the level list returned front; before it puts the mode back.
//   - "set.slotTaken" (item, bucket): Set of a key not present in a map with the embedded pool took item, a slot of the pool of bucket, from getWithFn; before it stores the reverse and the conflict of the key into item.
//   - "get.found" (item, nil): _get found item by searchKey; before it compares the reverse and the conflict of item with the key.
//   - "delete.found" (item, nil): Delete found item by LoadItem; before it runs Delete on item.
//   - "purge.lenLowered" (item, pool): purgeInEmbedded found item in the last slot of pool and lowered the length of pool over it; before it runs shrinkLen.
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

// StepLockBuckets returns the bucket that a lookup of reverse finds and the
// bucket that owns its item pool (toBase), so that a test can tell which
// muPool each operation on the key locks.
func StepLockBuckets(h *Map, reverse uint64) (found, base unsafe.Pointer) {
	b := h.findBucket(reverse)
	return unsafe.Pointer(b), unsafe.Pointer(b.toBase())
}

// StepPoolOf returns the item pool that the bucket found for reverse uses,
// and the reverses of the slots of that pool in the order of the slots.
func StepPoolOf(h *Map, reverse uint64) (pool unsafe.Pointer, reverses []uint64) {
	p := h.findBucket(reverse).itemPool()
	items := p.itemSlice(false)
	reverses = make([]uint64, items.Len())
	for i := range reverses {
		reverses[i] = items.reverseAt(i)
	}
	return unsafe.Pointer(p), reverses
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

// StepEntryReverse returns the reverse of the entry whose list node a
// StepHook point passed as p.
func StepEntryReverse(p unsafe.Pointer) uint64 {
	return EmptyMapHead.FromListHead((*elist_head.ListHead)(p)).reverse
}

// StepCheckPooledItems walks the list of entries of a map filled only by Set
// and reports the first item that lies outside the items of every pool that
// the pool lists of h hold. Such an item is kept alive by nothing.
func StepCheckPooledItems(h *Map) error {
	type span struct{ lo, hi uintptr }
	var spans []span
	if h.pooler != nil {
		for i := range h.pooler.itemPool {
			for cur := h.pooler.itemPool[i].DirectNext(); cur.DirectNext() != cur; cur = cur.DirectNext() {
				sp := samepleItemPoolFromListHead(cur)
				if cap(sp.items) == 0 {
					continue
				}
				lo := uintptr(unsafe.Pointer(&sp.items[:1][0]))
				spans = append(spans, span{lo, lo + uintptr(cap(sp.items))*SampleItemSize})
			}
		}
	}
	for cur := h.head.DirectNext(); cur != h.tail; cur = cur.DirectNext() {
		if EmptyMapHead.FromListHead(cur).IsDummy() {
			continue
		}
		p := uintptr(unsafe.Pointer(SampleItemFromListHead(cur)))
		found := false
		for _, s := range spans {
			if s.lo <= p && p < s.hi {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("entry %016x at %#x lies outside the items of every pool", EmptyMapHead.FromListHead(cur).reverse, p)
		}
	}
	return nil
}

// StepEntryLinked reports whether the list of entries of a map filled only by
// Set reaches the item at address p. p is a uintptr so that the caller does
// not keep the item alive.
func StepEntryLinked(h *Map, p uintptr) bool {
	for cur := h.head.DirectNext(); cur != h.tail; cur = cur.DirectNext() {
		if !EmptyMapHead.FromListHead(cur).IsDummy() && uintptr(unsafe.Pointer(SampleItemFromListHead(cur))) == p {
			return true
		}
	}
	return false
}

// StepIsLinkedPool reports whether p, a list node that a StepHook point of the
// pool passed, is the list node of a pool that a pool list of h holds now: not
// nil, not marked, and neither the head nor the tail of the list. Get may take
// an item only from such a pool.
func StepIsLinkedPool(h *Map, p unsafe.Pointer) bool {
	if p == nil || h.pooler == nil {
		return false
	}
	for i := range h.pooler.itemPool {
		for cur := h.pooler.itemPool[i].DirectNext().WithOutMark(); cur.DirectNext() != cur; cur = cur.DirectNext().WithOutMark() {
			if unsafe.Pointer(cur) == p {
				return true
			}
		}
	}
	return false
}

// StepCheckBuckets walks the buckets of a map with the embedded pool that a
// lookup can find (the top buckets and every element of their downLevels with
// a level above 0) and reports the first one that has no way to reach an item
// pool: no pool of its own, no parent and no itemPoolFn. itemPool of such a
// bucket calls itself until the stack overflows.
func StepCheckBuckets(h *Map) error {
	var walk func(b *bucket) error
	walk = func(b *bucket) error {
		if b._itemPool == nil && b._parent == nil && b.itemPoolFn == nil {
			return fmt.Errorf("bucket (reverse %016x, level %d) has no item pool", b.reverse, b.level())
		}
		downs := b.ptrDownLevels()
		for i := 0; i < downs.Len(); i++ {
			if d := downs.at(i); d != nil && d.level() > 0 {
				if err := walk(d); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range h.buckets {
		if err := walk(&h.buckets[i]); err != nil {
			return err
		}
	}
	return nil
}
