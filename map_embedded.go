// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"fmt"
	"math/bits"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap/atomic_util"
)

func (h *Map) bsearchBybucket(bucket *bucket, reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry {

	stepAt("bsearch.begin", unsafe.Pointer(bucket), nil)
	pool := bucket.toBase().itemPool()
	// FIXME: why fail to get
	if pool == nil {
		pool = bucket.toBase().itemPool()
	}
	// MENTION: should remove 0 slice ?
	//items := pool.itemSlice(false)
	items := pool.ptrItems()
	// insertToPool may put a new array into the pool while the search reads
	// the old one; the search starts again when the array changed under it
	for {
		version := pool.publication.Load()
		data := atomic.LoadPointer(&items.data)
		l := items.Len()
		stepAt("bsearch.snapshot", unsafe.Pointer(pool), nil)
		if version&1 != 0 {
			runtime.Gosched()
			continue
		}

		idx := sort.Search(l, func(i int) bool {
			item := items._at(i, true, true)
			if item == nil {
				return true
			}
			return atomic.LoadUint64(&item.reverse) >= reverseNoMask
			//return items.reverseAt(i) >= reverseNoMask
		})
		stepAt("bsearch.searched", unsafe.Pointer(bucket), nil)
		// Equal reverses sit next to each other; a purged placeholder may
		// precede the live entry of the same key, so skip ignored matches.
		var found *SampleItem
		for ; idx < l && items.reverseAt(idx) == reverseNoMask; idx++ {
			item := items._at(idx, true, false)
			if item == nil {
				break
			}
			if ignoreBucketEnry && item.IsIgnored() {
				continue
			}
			found = item
			break
		}
		if atomic.LoadPointer(&items.data) != data || pool.publication.Load() != version {
			continue
		}
		if found != nil {
			return found
		}
		break
	}

	a := bucket.toBase().prevAsB()
	if a.reverse < reverseNoMask {
		if nb := h.findBucket(reverseNoMask); nb.toBase() != bucket.toBase() {
			return h.bsearchBybucket(nb, reverseNoMask, ignoreBucketEnry)
		}
	}

	return nil
}

func (h *Map) searchKeyFromEmbeddedPool(k uint64, ignoreBucketEnry bool) HMapEntry {
	rev := bits.Reverse64(k)
	//return h.searchByEmbeddedbucket(h.findBucket(rev), rev, ignoreBucketEnry)
	return h.bsearchBybucket(h.findBucket(rev), rev, ignoreBucketEnry)

}

func (h *Map) makeBucket2(bucket *bucket) (err error) {
	atomic.AddInt32(&madeBucket, 1)

	nextBucket := bucket.prevAsB()

	nextReverse := nextBucket.reverse

	newReverse := nextReverse/2 + bucket.reverse/2
	if newReverse&1 > 0 {
		newReverse++
	}
	// Decide whether a split is possible before claiming a slot of the bucket table.
	idx, err := bucket.itemPool().findIdx(newReverse)
	if err != nil || idx == 0 {
		return err
	}

	b := h.bucketFromPoolEmbedded(newReverse)
	if b == nil {
		return ErrBucketAllocatedFail
	}
	stepAt("makeBucket2.got", unsafe.Pointer(b), unsafe.Pointer(bucket))

	if b.reverse == 0 && b.level() > 1 {
		err = NewError(EBucketInvalid, "bucket.reverse = 0. but level 1= 1", nil)
		Log(LogWarn, "%s", err.Error())
		return
	}
	b.Init()
	b.LevelHead.Init()
	b.initItemPool()

	olen := len(bucket.itemPool().items)
	nPool, err2 := bucket.itemPool()._split(idx, false)
	_ = err2
	b.setItemPool(nPool)
	atomic.StoreInt32(&bucket._len, int32(idx))
	atomic.StoreInt32(&b._len, int32(olen-idx))

	h.addBucket(b)
	stepAt("makeBucket2.added", unsafe.Pointer(b), unsafe.Pointer(bucket))
	if l := b.level(); l < 0 {
		b.setLevel(-l)
	}
	spItems := bucket.itemPool().ptrItems()
	atomic_util.StoreInt(&spItems.len, idx)
	atomic_util.StoreInt(&spItems.cap, idx)

	if b.LevelHead.DirectNext() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	h.insertOnLevel(b, b.level(), "makeBucket2.levelFound", unsafe.Pointer(b))
	if b.LevelHead.Next() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	stepAt("makeBucket2.recurse", unsafe.Pointer(bucket), unsafe.Pointer(b))
	if int(b.len()) > h.maxPerBucket {
		h.makeBucket2(b)
	} else if int(bucket.len()) > h.maxPerBucket {
		h.makeBucket2(bucket)
	}

	return nil
}

func (h *Map) bucketFromPoolEmbedded(reverse uint64) (b *bucket) {

	claimed := false
	level := int32(0)
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	for l := int32(1); l <= level; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		var downs *bucketSlice
		downs = b.ptrDownLevels()

		// init downLevels[0]
		if downs == nil || atomic_util.CompareAndSwapInt(&downs.cap, 0, 1) {
			//if cap(b.downLevels) == 0 {
			downLevels := make([]bucket, 0, 16)
			atomic.StorePointer(&downs.data, unsafe.Pointer(unsafe.SliceData(downLevels)))
			atomic_util.StoreInt(&downs.cap, cap(downLevels))
			downs = b.ptrDownLevels()
			if atomic_util.LoadInt(&downs.len) == 1 {
				goto SKIP_FIRST_DOWN_INIT
			}
			firstDown := downs._at(0, false)
			firstDown.setLevel(b.childLevel())
			firstDown.reverse = b.reverse
			firstDown.Init()
			firstDown.LevelHead.Init()
			firstDown._parent = b

			firstDown.setItemPoolFn = func(p *samepleItemPool) {
				b.setItemPool(p)
			}

			h.insertOnLevel(firstDown, l, "bucketFromPoolEmbedded.levelFound", unsafe.Pointer(b))
			if !atomic_util.CompareAndSwapInt(&downs.len, 0, 1) {
				panic("this must not be reached")
			}
		}
	SKIP_FIRST_DOWN_INIT:
		downs = b.ptrDownLevels()
		for {
			len := atomic_util.LoadInt(&downs.len)
			if len <= idx && atomic_util.CompareAndSwapInt(&downs.len, len, idx+1) {
				break
			} else if len > idx {
				break
			}
			Log(LogWarn, "downs.len is updated. retry")
		}

		// init downLevels[idx]; of two splits of the same gap, the one that
		// sets the level of the element takes it
		if downs.at(idx).level() == 0 {
			if stepEnabled {
				stepAt("bucketFromPoolEmbedded.claim", unsafe.Pointer(downs.at(idx)), unsafe.Pointer(b))
			}
			if l != level {
				Log(LogWarn, "not collected already inited")
			}
			if !atomic.CompareAndSwapInt32(&downs.at(idx)._level, 0, -b.childLevel()) {
				// another split took the element
				return nil
			}
			downs.at(idx).reverse = b.reverse | (uint64(idx) << (4 * (16 - l)))
			b = downs.at(idx)
			stepAt("bucketFromPoolEmbedded.claimed", unsafe.Pointer(b), nil)
			if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
				Log(LogWarn, "already inited")
			}
			claimed = true
			break
		}
		b = downs.at(idx)
	}
	if !claimed {
		// the bucket of reverse is made already; a split must not make it
		// again
		return nil
	}
	if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
		h.DumpBucket(logio)
		Log(LogWarn, "already inited")
	}
	return

}

func (b *bucket) toBase() *bucket {

	if b._parent == nil {
		return b
	}
	return b._parent.toBase()
}

const (
	getEmpty      byte = 1
	getLargest         = 2
	getNoCap           = 3
	requireInsert      = 4
	foundFree          = 5
)

func (sp *samepleItemPool) len() (l int) {
	return sp.ptrItems().Len()

}

func (sp *samepleItemPool) cap() (l int) {
	return sp.ptrItems().Cap()
}

func (sp *samepleItemPool) state4get(reverse uint64, len int, cap int) byte {

	if len == 0 {
		return getEmpty
	}

	if _, found := sp.bsearchFromFreeList(reverse); found {
		return foundFree
	}

	if cap == len {
		return getNoCap
	}

	// deleted items keep their place in the order, so compare with the last one
	items := sp.itemSlice(false)
	last := items._at(len-1, false, false)
	if atomic.LoadUint64(&last.reverse) < reverse {
		return getLargest
	}

	return requireInsert

}

// bsearchFromFreeList returns a deleted item that can take reverse without
// breaking the order of items, deleted ones included: the last item not
// above reverse, or items[0] when every item is above it.
func (sp *samepleItemPool) bsearchFromFreeList(reverse uint64) (int, bool) {

	items := sp.ptrItems()

	idx := sort.Search(items.Len(), func(i int) bool {
		item := items._at(i, true, false)
		return atomic.LoadUint64(&item.reverse) > reverse
	})
	if idx < 1 {
		idx = 1
	}
	mItem := items._at(idx-1, true, false)

	if mItem != nil && mItem.IsDeleted() {
		//mItem.Init()
		return idx - 1, true
	}
	return -1, false
}

var lastgets []byte = nil

func lazyUnlock(mu sync.Locker) {
	if mu != nil {
		mu.Unlock()
	}
}

type unlocker func(mu sync.Locker)

// appendLast takes the slot after the last one for the key of reverse. It
// writes reverse into the slot before the slot counts in the length, so that
// a binary search over the slots never meets a slot of reverse 0 there.
func (sp *samepleItemPool) appendLast(reverse uint64, mu sync.Locker) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {

	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	var new *SampleItem
	items := sp.ptrItems()
	l := items.Len()
	if l >= items.Cap() {
		return nil, nil, fn
	}
	// the slot may be one released by purgeInEmbedded: clear its old
	// identity and give it reverse while it is past the length, where no
	// reader looks at it yet
	slot := items._at(l, false, false)
	atomic.StoreUint32((*uint32)(&slot.PtrMapHead().state), 0)
	atomic.StoreUint64(&slot.PtrMapHead().conflict, 0)
	atomic.StoreUint64(&slot.PtrMapHead().reverse, reverse)
	if atomic_util.CompareAndSwapInt(&items.len, l, l+1) {
		new = items.at(l)
		stepAt("appendLast.claimed", unsafe.Pointer(new), nil)
		return new, nil, fn
	}
	Log(LogWarn, "retry to fail to expand")
	return nil, nil, fn
}

func (sp *samepleItemPool) insertToPool(reverse uint64, mu sync.Locker) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	ocap := cap(sp.items)

	// require insert
	if IsDebug() {
		var b strings.Builder
		for i := range sp.items {
			sp.items[i].PtrMapHead().dump(&b)
		}
		Log(LogDebug, "B: itemPool.items\n%s\n", b.String())
	}
	// FIXME: should disable not IsDebug()
	//sp.validateItems()
	if olen != len(sp.items) {
		Log(LogDebug, "update olen")
		newItem, nPool, _ = sp.getWithFn(reverse, nil)
		return newItem, nPool, fn
	}
	//olen = len(sp.items)
	nlen := int64(olen)

	for i := 0; i < olen; i++ {
		//for i := range sp.items {
		if sp.items[i].reverse < reverse {
			continue
		}
		if i == olen-1 {
			//fmt.Printf("invalid")
		}
		if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
			Log(LogDebug, "update olen")
			newItem, nPool, _ = sp.getWithFn(reverse, nil)
			return newItem, nPool, fn
		}
		var err error
		head, tail := sp.linkedEnds(olen)
		prevItem := sp.items[head].ListHead.Prev()
		nextItem := sp.items[tail].ListHead.Next()

		// copy to new slice
		newItems := make([]SampleItem, olen+1, maxInts(ocap, olen+1))
		if i > 0 {
			copy(newItems[0:i], sp.items[0:i])
		}
		copy(newItems[i+1:], sp.items[i:])

		// Links are offsets, so every ListHead they reach must live in memory
		// that never moves. Goroutine stacks move; only newItems (heap) and the
		// list neighbours are linked here.
		middle := nextListHeadOfSampleItem()
		for i := 0; i < olen+1; i++ {
			newItems[i].ListHead = middle
		}

		first := &newItems[0].ListHead
		if i == 0 {
			first = &newItems[1].ListHead
		} else {
			before, after := holeListHeadsOfSampleItem()
			newItems[i-1].ListHead = before
			newItems[i+1].ListHead = after
		}
		newItems[i].Init()
		// the new slot has the reverse of its key before the array is
		// published, so that the slots stay in order for a binary search
		newItems[i].PtrMapHead().reverse = reverse

		err = prevItem.ReplaceNext(first, &newItems[olen].ListHead, nextItem)
		if err != nil {
			Log(LogFatal, "fail to replace newItems")
		}

		oldItems := sp.ptrItems().dup()
		newItemSlice := toItemSlice(newItems)
		stepAt("insertToPool.publish", unsafe.Pointer(sp), nil)
		sp.publishItems(&newItemSlice)

		// for debug
		oldItemFirst := oldItems.at(0).Prev().Next().PtrMapHead()
		oldItemNext := oldItems.at(olen - 1).Next().PtrMapHead()
		_ = oldItemFirst
		_ = oldItemNext
		ItemNext := sp.items[olen].Next().PtrMapHead()
		_ = ItemNext

		if IsDebug() {
			var b strings.Builder
			for i := range sp.items {
				sp.items[i].PtrMapHead().dump(&b)
			}
			fmt.Printf("A: itemPool.items\n%s\n", b.String())
		}

		outside := sp.items[olen].Next()
		_ = outside
		if olen != i && olen-1 != i && olen-1 > 0 && sp.items[olen-1].PtrListHead().Next() != sp.items[olen].PtrListHead() {
			toNext := sp.items[olen-1].PtrListHead().Next()
			next := sp.items[olen].PtrListHead()
			Log(LogFatal, "not connect sp.items[olen-1]=%p -> sp.items[olen]=%p ", toNext, next)
		}
		if olen != i && olen-1 != i && olen-1 > 0 && sp.items[olen].PtrListHead().Prev() != sp.items[olen-1].PtrListHead() {
			c := sp.items[olen].PtrListHead().Prev()
			p := sp.items[olen-1].PtrListHead()
			Log(LogFatal, "not connect sp.items[olen-1]=%p <- sp.items[olen]=%p", p, c)
		}

		return &sp.items[i], nil, lazyUnlock
	}
	return sp.getWithFn(reverse, nil)
	//return nil, nil, nil

}

//go:norace
func (sp *samepleItemPool) getWithFn(reverse uint64, mu sync.Locker) (new MapItem, nPool *samepleItemPool, fn unlocker) {

	items := *sp.ptrItems()
	olen := items.Len()
	ocap := items.Cap()

	defer func() {
		if new == nil {
			Log(LogWarn, "getWithFn(): item is nil")
		}

	}()

	// for debug
	//lastgets = append(lastgets, sp.state4get(reverse, lastActiveIdx))
	switch sp.state4get(reverse, olen, ocap) {
	case getEmpty, getLargest:
		nmu := mu
		new, nPool, fn = sp.appendLast(reverse, nmu)
		if new != nil {
			return
		}
		for {
			if fn != nil {
				nmu = nil
			}
			if nPool == nil {
				nPool = sp
			}
			// fn stays: the lock, if any, was taken by appendLast above
			new, nPool, _ = nPool.getWithFn(reverse, nmu)
			if new != nil {
				return
			}

		}
	case getNoCap:
		fn, err := sp.expand(mu)
		if err != nil {
			Log(LogWarn, "pool.expand() require retry")
		}
		new, nPool, _ = sp.getWithFn(reverse, nil)
		return new, nPool, fn
	case foundFree:
		// hold the bucket lock while the slot is reclaimed, like appendLast
		// and insertToPool; the caller runs fn to release it
		if mu != nil {
			mu.Lock()
			fn = lazyUnlock
		}

		idx, found := sp.bsearchFromFreeList(reverse)
		if !found {
			goto RETRY
		}
		items := sp.itemSlice(false)
		new = items._at(idx, true, false)
		if new == nil {
			goto RETRY
		}
		oState := atomic.LoadUint32((*uint32)(&(new.PtrMapHead().state)))
		oReverse := atomic.LoadUint64(&new.PtrMapHead().reverse)

		if oState&uint32(mapIsDeleted) == 0 {
			goto RETRY
		}
		// the free slot takes the reverse of the new key at once, so that
		// the slots stay in order for a binary search
		if !atomic.CompareAndSwapUint64(&new.PtrMapHead().reverse, oReverse, reverse) {
			goto RETRY
		}
		atomic.StoreUint64(&new.PtrMapHead().conflict, 0)

		if !atomic.CompareAndSwapUint32((*uint32)(&(new.PtrMapHead().state)), oState, uint32(mapIsDeleted)) {
			atomic.StoreUint64(&new.PtrMapHead().reverse, oReverse)
			goto RETRY
		}
		new.PtrListHead().MarkForDelete()
		return new, nil, fn
	}

	return sp.insertToPool(reverse, mu)

RETRY:
	if fn != nil {
		mu.Unlock()
	}
	return sp.getWithFn(reverse, mu)

}

// linkedEnds returns the first and the last of sp.items[:n] that are in the
// list: purged items stay in the array unlinked. insertToPool and expand
// call it with a live item in sp.items[:n].
func (sp *samepleItemPool) linkedEnds(n int) (head, tail int) {
	head, tail = 0, n-1
	for sp.items[head].ListHead.Empty() {
		head++
	}
	for sp.items[tail].ListHead.Empty() {
		tail--
	}
	return
}

func (sp *samepleItemPool) expand(mu sync.Locker) (unlocker, error) {
	var fn unlocker
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	//ocap := cap(sp.items)

	if olen != len(sp.items) {
		Log(LogDebug, "update olen")
		_, e := sp.expand(nil)
		return fn, e
	}
	nlen := int64(olen)

	if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
		Log(LogDebug, "update olen")
		_, e := sp.expand(nil)
		return fn, e
	}

	var err error
	head, tail := sp.linkedEnds(olen)
	prevItem := sp.items[head].ListHead.Prev()
	nextItem := sp.items[tail].ListHead.Next()

	nCap := PoolCap(len(sp.items))

	newItems := make([]SampleItem, olen, nCap)
	toPtrItemSlice(&newItems).CopyDataFrom(0, sp.ptrItems(), 0, olen)

	// Links are offsets; the copied ends still point relative to the old
	// array, so connect them straight to the heap neighbours (no stack heads).
	err = prevItem.ReplaceNext(&newItems[head].ListHead, &newItems[tail].ListHead, nextItem)
	if err != nil {
		Log(LogFatal, "fail to replace newItems")
	}

	oldItems := sp.ptrItems().dup()
	sp.publishItems(toPtrItemSlice(&newItems))

	// for debug
	oldItemFirst := oldItems.at(0).Prev().Next().PtrMapHead()
	oldItemNext := oldItems.at(olen - 1).Next().PtrMapHead()
	_ = oldItemFirst
	_ = oldItemNext
	ItemNext := sp.items[olen-1].Next().PtrMapHead()
	_ = ItemNext

	if IsDebug() {
		var b strings.Builder
		for i := range sp.items {
			sp.items[i].PtrMapHead().dump(&b)
		}
		fmt.Printf("A: itemPool.items\n%s\n", b.String())
	}

	return fn, nil

}

// nextListHeadOfSampleItem returns the links of an item whose neighbours are
// the adjacent slots of the same []SampleItem. All three scratch items live in
// one array, so no offset crosses into memory that can move.
func nextListHeadOfSampleItem() elist_head.ListHead {

	items := make([]SampleItem, 3)

	elist_head.InitAsEmpty(items[0].PtrListHead(), items[2].PtrListHead())
	items[2].InsertBefore(items[1].PtrListHead())

	return items[1].ListHead
}

// holeListHeadsOfSampleItem returns the links of the slots just before and just
// after an unlinked slot, so the chain skips that slot.
func holeListHeadsOfSampleItem() (before, after elist_head.ListHead) {

	items := make([]SampleItem, 5)

	elist_head.InitAsEmpty(items[0].PtrListHead(), items[4].PtrListHead())
	items[4].InsertBefore(items[3].PtrListHead())
	items[3].InsertBefore(items[1].PtrListHead())

	return items[1].ListHead, items[3].ListHead
}

func (sp *samepleItemPool) findIdx(reverse uint64) (int, error) {

	for i := range sp.items {
		if reverse <= sp.items[i].reverse {
			return i, nil
		}
	}
	return -1, ErrIdxOverflow

}

func (sp *samepleItemPool) split(idx int) (nPool *samepleItemPool, err error) {
	return sp._split(idx, true)

}
func (sp *samepleItemPool) _split(idx int, connect bool) (nPool *samepleItemPool, err error) {

	nlen := int64(len(sp.items))
	if int(nlen) <= idx {
		return nil, ErrIdxOverflow
	}

	nPool = &samepleItemPool{}

	if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
		return sp._split(idx, connect)
	}

	// sp keeps its items: the caller shrinks sp after nPool's bucket is readable.
	spItems := sp.ptrItems()
	nPool.ptrItems().CopyFrom(spItems, idx, spItems.Len()-idx)
	nPool.Init()
	//sp.validateItems()
	//nPool.validateItems()

	if connect && sp.PtrListHead().Next() != nil && sp.PtrListHead().Next() != sp.PtrListHead() {
		_, err = sp.PtrListHead().Next().InsertBefore(nPool.PtrListHead())
	}
	return

}

func (sp *samepleItemPool) initFreeList() {

	elist_head.InitAsEmpty(&sp.freeHead, &sp.freeTail)
}

func (sp *samepleItemPool) PushWithOrder(item *SampleItem) error {

	p := uintptr(unsafe.Pointer(item.PtrListHead()))

	if sp.freeHead.Empty() {
		sp.initFreeList()
	}

	if sp.freeHead.DirectNext() == &sp.freeTail {
		goto ADD_LAST
	}

	for cur := sp.freeHead.Next(); cur != &sp.freeTail; cur = cur.Next() {
		if uintptr(unsafe.Pointer(cur)) < p {
			continue
		}
		_, err := cur.InsertBefore(item.PtrListHead())
		return err
	}

ADD_LAST:
	_, err := sp.freeTail.InsertBefore(item.PtrListHead())
	return err
}

func (sp *samepleItemPool) shrinkLen() {

	pItemSlice := sp.ptrItems()
	l := pItemSlice.Len()
	for i := l - 1; i > -1; i-- {
		item := pItemSlice._at(i, true, false)
		if item == nil || !item.IsDeleted() {
			break
		}
		if !atomic_util.CompareAndSwapInt(&pItemSlice.len, l, i) {
			break
		}
		l = i
		if !item.Empty() {
			item.MarkForDelete()
		}
	}
}

type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}
type itemSlice struct {
	sliceHeader
}

const sampleItemItemsOffset = unsafe.Offsetof(EmptysamepleItemPool.items)
const itemSize = unsafe.Sizeof(SampleItem{})

func toPtrItemSlice(items *[]SampleItem) (list *itemSlice) {
	return (*itemSlice)(unsafe.Pointer(items))
}

func toItemSlice(items []SampleItem) (list itemSlice) {
	slice := (*itemSlice)(unsafe.Pointer(&items))
	list.data = atomic.LoadPointer(&slice.data)
	list.len = atomic_util.LoadInt(&slice.len)
	list.cap = atomic_util.LoadInt(&slice.cap)
	return
}

func (sp *samepleItemPool) ptrItems() (result *itemSlice) {
	return (*itemSlice)(unsafe.Add(unsafe.Pointer(sp), sampleItemItemsOffset))
}

// publishItems replaces the array under the bucket lock. An odd publication
// means readers cannot yet pair its data pointer with its length.
func (sp *samepleItemPool) publishItems(items *itemSlice) {
	sp.publication.Add(1)
	sp.ptrItems().CopyFrom(items, 0, items.Len())
	sp.publication.Add(1)
}

func (sp *samepleItemPool) itemSlice(isNoneZero bool) (result itemSlice) {
	for {
		//result = *sp.ptrItems()
		ptr := sp.ptrItems()
		result.data = atomic.LoadPointer(&ptr.data)
		result.len = atomic_util.LoadInt(&ptr.len)
		result.cap = atomic_util.LoadInt(&ptr.cap)

		if result.data != nil {
			break
		}
		if !isNoneZero && result.Len()+result.Cap() == 0 {
			break
		}
	}
	return
}

func (list *itemSlice) at(i int) (result *SampleItem) {

	return list._at(i, true, false)
}

func (list *itemSlice) _at(i int, checklen bool, skipOnDelete bool) (result *SampleItem) {

	if checklen && atomic_util.LoadInt(&list.len) <= i {
		return nil
	} else if atomic_util.LoadInt(&list.cap) <= i {
		return nil
	}

	data := atomic.LoadPointer(&list.data)
	pCur := unsafe.Add(data, i*int(itemSize))
	if !skipOnDelete {
		return (*SampleItem)(pCur)
	}

	for {
		result = (*SampleItem)(pCur)
		if !result.IsDeleted() {
			break
		}
		if pCur == data {
			break
		}
		pCur = unsafe.Add(pCur, -int(itemSize))
	}

	return result
}

func (list *itemSlice) Len() int {

	return atomic_util.LoadInt(&list.len)
}

func (list *itemSlice) Cap() int {

	return atomic_util.LoadInt(&list.cap)
}
func (list *itemSlice) init() {
	atomic.StorePointer(&list.data, nil)
	atomic_util.StoreInt(&list.len, 0)
	atomic_util.StoreInt(&list.cap, 0)

}

//go:norace
func (list *itemSlice) CopyFrom(slist *itemSlice, head, len int) {
	scap := atomic_util.LoadInt(&slist.cap)

	old := *list
	_ = old

	// Readers load len before data (_at, bsearchBybucket). Publish data first
	// so a reader never pairs the new, larger len with the old array.
	if !atomic.CompareAndSwapPointer(&list.data, list.data, unsafe.Pointer(slist._at(head, false, false))) {
		goto FAIL
	}
	stepAt("slice.dataPublished", unsafe.Pointer(list), nil)
	if !atomic_util.CompareAndSwapInt(&list.cap, list.cap, scap-head) {
		goto FAIL
	}
	if !atomic_util.CompareAndSwapInt(&list.len, list.len, len) {
		goto FAIL
	}

	return

FAIL:
	panic("fail copy")
}

func itemSliceTobytes(list *itemSlice, idx, len int) (bytes []byte) {

	return ptrTobytes(unsafe.Pointer(list._at(idx, false, false)),
		list.Len()*int(itemSize),
		(list.Cap()-idx)*int(itemSize))

}

func ptrTobytes(ptr unsafe.Pointer, len, cap int) (bytes []byte) {

	list := (*sliceHeader)(unsafe.Pointer(&bytes))

	if !atomic_util.CompareAndSwapInt(&list.len, 0, len) {
		goto FAIL
	}
	if !atomic_util.CompareAndSwapInt(&list.cap, 0, cap) {
		goto FAIL
	}
	if !atomic.CompareAndSwapPointer(&list.data, nil, ptr) {
		goto FAIL
	}

	return

FAIL:
	panic("fail copy")
}

func (list *itemSlice) CopyDataFrom(head int, slist *itemSlice, shead, slen int) {

	sbytes := itemSliceTobytes(slist, shead, slen)
	bytes := itemSliceTobytes(list, head, slen)
	copy(bytes, sbytes)
}

func (list *itemSlice) dup() (new *itemSlice) {
	new = &itemSlice{}
	new.init()
	new.CopyFrom(list, 0, list.Len())
	return
}

func (list *itemSlice) reverseAt(idx int) (r uint64) {

	const toReverse = unsafe.Offsetof(EmptySampleHMapEntry.reverse)
	ptr := unsafe.Add(atomic.LoadPointer(&list.data), idx*int(SampleItemSize)+int(toReverse))
	r = atomic.LoadUint64((*uint64)(ptr))
	return r
}

func (list *itemSlice) reduceCap() int {

	return 0
}
