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

func (h *Map[K, V]) bsearchBybucket(bucket *bucket[K, V], reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry[K, V] {

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
		if version&1 != 0 || pool.publication.Load() != version {
			runtime.Gosched()
			continue
		}
		snapshot := unsafe.Slice((*SampleItem[K, V])(data), l)

		idx := sort.Search(l, func(i int) bool {
			item := skipDeletedItems[K, V](&snapshot[i], &snapshot[0])
			return atomic.LoadUint64(&item.reverse) >= reverseNoMask
			//return items.reverseAt(i) >= reverseNoMask
		})
		stepAt("bsearch.searched", unsafe.Pointer(bucket), nil)
		// Equal reverses sit next to each other; a purged placeholder may
		// precede the live entry of the same key, so skip ignored matches.
		var found *SampleItem[K, V]
		for ; idx < l && atomic.LoadUint64(&snapshot[idx].reverse) == reverseNoMask; idx++ {
			item := &snapshot[idx]
			// A reused slot can expose its new hash before Set links it.
			// Such a slot is not yet a search result, even if its state is live.
			if ignoreBucketEnry && (item.IsIgnored() || !linkedEntry(item.PtrListHead())) {
				continue
			}
			found = item
			break
		}
		if pool.publication.Load() != version {
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

func (h *Map[K, V]) searchKeyFromEmbeddedPool(k uint64, ignoreBucketEnry bool) HMapEntry[K, V] {
	rev := bits.Reverse64(k)
	//return h.searchByEmbeddedbucket(h.findBucket(rev), rev, ignoreBucketEnry)
	return h.bsearchBybucket(h.findBucket(rev), rev, ignoreBucketEnry)

}

func (h *Map[K, V]) makeBucket2(bucket *bucket[K, V]) (err error) {
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

	// The child has a separate pool mutex. Hold it before publication so
	// writers cannot expand its pool while this split still changes its slice.
	b.muPool.Lock()
	defer b.muPool.Unlock()
	h.addBucket(b)
	stepAt("makeBucket2.added", unsafe.Pointer(b), unsafe.Pointer(bucket))
	if level := b.level(); level < 0 {
		b.setLevel(-level)
	}
	spItems := bucket.itemPool().ptrItems()
	atomic_util.StoreInt(&spItems.len, idx)
	atomic_util.StoreInt(&spItems.cap, idx)

	h.insertOnLevel(b, b.level(), "makeBucket2.levelFound", unsafe.Pointer(b))
	stepAt("makeBucket2.recurse", unsafe.Pointer(bucket), unsafe.Pointer(b))
	if int(b.len()) > h.maxPerBucket {
		h.makeBucket2(b)
	} else if int(bucket.len()) > h.maxPerBucket {
		h.makeBucket2(bucket)
	}

	return nil
}

func (h *Map[K, V]) bucketFromPoolEmbedded(reverse uint64) (b *bucket[K, V]) {

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
		var downs *bucketSlice[K, V]
		downs = b.ptrDownLevels()

		// init downLevels[0]
		if downs == nil || atomic_util.CompareAndSwapInt(&downs.cap, 0, 1) {
			//if cap(b.downLevels) == 0 {
			downLevels := make([]bucket[K, V], 0, 16)
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

			firstDown.setItemPoolFn = func(p *samepleItemPool[K, V]) {
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

func (b *bucket[K, V]) toBase() *bucket[K, V] {

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

func (sp *samepleItemPool[K, V]) len() (l int) {
	return sp.ptrItems().Len()

}

func (sp *samepleItemPool[K, V]) cap() (l int) {
	return sp.ptrItems().Cap()
}

func (sp *samepleItemPool[K, V]) state4get(reverse uint64, len int, cap int) byte {

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
func (sp *samepleItemPool[K, V]) bsearchFromFreeList(reverse uint64) (int, bool) {

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
func (sp *samepleItemPool[K, V]) appendLast(reverse uint64, mu sync.Locker) (newItem MapItem[K, V], nPool *samepleItemPool[K, V], fn unlocker) {

	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	var new *SampleItem[K, V]
	items := sp.ptrItems()
	l := items.Len()
	if l >= items.Cap() {
		return nil, nil, fn
	}
	// the slot may be one released by purgeInEmbedded: clear its old
	// identity and give it reverse while it is past the length, where no
	// reader looks at it yet
	slot := items._at(l, false, false)
	atomic.AndUint64((*uint64)(&slot.PtrMapHead().state), ^uint64(mapIsDummy|mapIsDeleted|mapIsPoolItem|mapIsBusy))
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

func (sp *samepleItemPool[K, V]) insertToPool(reverse uint64, mu sync.Locker) (newItem MapItem[K, V], nPool *samepleItemPool[K, V], fn unlocker) {
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
			sp.items[i].PtrMapHead().dump[K, V](&b)
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
		newItems := newPoolItems[K, V](olen+1, maxInts(ocap, olen+1), true)
		if i > 0 {
			copyPoolItems(newItems[0:i], sp.items[0:i])
		}
		copyPoolItems(newItems[i+1:], sp.items[i:])

		first, last := linkPoolItems(newItems, i)
		newItems[i].PtrMapHead().reverse = reverse
		err = prevItem.ReplaceNext(&newItems[first].ListHead, &newItems[last].ListHead, nextItem)
		if err != nil {
			Log(LogFatal, "fail to replace newItems")
		}

		oldItems := sp.ptrItems().dup()
		newItemSlice := toItemSlice[K, V](newItems)
		stepAt("insertToPool.publish", unsafe.Pointer(sp), nil)
		sp.publishItems(&newItemSlice)

		// for debug
		_ = oldItems

		if IsDebug() {
			var b strings.Builder
			for i := range sp.items {
				sp.items[i].PtrMapHead().dump[K, V](&b)
			}
			fmt.Printf("A: itemPool.items\n%s\n", b.String())
		}

		if olen != i && olen-1 != i && olen-1 > 0 && !sp.items[olen-1].IsIgnored() && !sp.items[olen].IsIgnored() && sp.items[olen-1].PtrListHead().Next() != sp.items[olen].PtrListHead() {
			toNext := sp.items[olen-1].PtrListHead().Next()
			next := sp.items[olen].PtrListHead()
			Log(LogFatal, "not connect sp.items[olen-1]=%p -> sp.items[olen]=%p ", toNext, next)
		}
		if olen != i && olen-1 != i && olen-1 > 0 && !sp.items[olen-1].IsIgnored() && !sp.items[olen].IsIgnored() && sp.items[olen].PtrListHead().Prev() != sp.items[olen-1].PtrListHead() {
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
func (sp *samepleItemPool[K, V]) getWithFn(reverse uint64, mu sync.Locker) (new MapItem[K, V], nPool *samepleItemPool[K, V], fn unlocker) {

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
		oState := atomic.LoadUint64((*uint64)(&(new.PtrMapHead().state)))
		oReverse := atomic.LoadUint64(&new.PtrMapHead().reverse)

		if oState&uint64(mapIsDeleted) == 0 {
			goto RETRY
		}
		// the free slot takes the reverse of the new key at once, so that
		// the slots stay in order for a binary search
		if !atomic.CompareAndSwapUint64(&new.PtrMapHead().reverse, oReverse, reverse) {
			goto RETRY
		}
		atomic.StoreUint64(&new.PtrMapHead().conflict, 0)

		if !atomic.CompareAndSwapUint64((*uint64)(&(new.PtrMapHead().state)), oState, (oState&^uint64(mapIsDummy|mapIsDeleted|mapIsPoolItem|mapIsBusy))|uint64(mapIsDeleted)) {
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
func (sp *samepleItemPool[K, V]) linkedEnds(n int) (head, tail int) {
	head, tail = 0, n-1
	for sp.items[head].ListHead.Empty() {
		head++
	}
	for sp.items[tail].ListHead.Empty() {
		tail--
	}
	base := uintptr(unsafe.Pointer(&sp.items[0].ListHead))
	stride := SampleItemSize[K, V]()
	index := func(link *elist_head.ListHead) (int, bool) {
		delta := uintptr(unsafe.Pointer(link)) - base
		return int(delta / stride), delta < uintptr(n)*stride && delta%stride == 0
	}
	for {
		i, inside := index(sp.items[head].ListHead.Prev())
		if !inside {
			break
		}
		head = i
	}
	for {
		i, inside := index(sp.items[tail].ListHead.Next())
		if !inside {
			break
		}
		tail = i
	}
	return
}

func (sp *samepleItemPool[K, V]) expand(mu sync.Locker) (unlocker, error) {
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

	newItems := newPoolItems[K, V](olen, nCap, true)
	toPtrItemSlice[K, V](&newItems).CopyDataFrom(0, sp.ptrItems(), 0, olen)

	first, last := linkPoolItems(newItems, -1)
	err = prevItem.ReplaceNext(&newItems[first].ListHead, &newItems[last].ListHead, nextItem)
	if err != nil {
		Log(LogFatal, "fail to replace newItems")
	}

	oldItems := sp.ptrItems().dup()
	sp.publishItems(toPtrItemSlice[K, V](&newItems))

	// for debug
	_ = oldItems

	if IsDebug() {
		var b strings.Builder
		for i := range sp.items {
			sp.items[i].PtrMapHead().dump[K, V](&b)
		}
		fmt.Printf("A: itemPool.items\n%s\n", b.String())
	}

	return fn, nil

}

// nextListHeadOfSampleItem returns the links of an item whose neighbours are
// the adjacent slots of the same []SampleItem. All three scratch items live in
// one array, so no offset crosses into memory that can move.
func nextListHeadOfSampleItem[K Key[K], V any]() elist_head.ListHead {

	items := make([]SampleItem[K, V], 3)

	elist_head.InitAsEmpty(items[0].PtrListHead(), items[2].PtrListHead())
	items[2].InsertBefore(items[1].PtrListHead())

	return items[1].ListHead
}

// holeListHeadsOfSampleItem returns the links of the slots just before and just
// after an unlinked slot, so the chain skips that slot.
func holeListHeadsOfSampleItem[K Key[K], V any]() (before, after elist_head.ListHead) {

	items := make([]SampleItem[K, V], 5)

	elist_head.InitAsEmpty(items[0].PtrListHead(), items[4].PtrListHead())
	items[4].InsertBefore(items[3].PtrListHead())
	items[3].InsertBefore(items[1].PtrListHead())

	return items[1].ListHead, items[3].ListHead
}

func (sp *samepleItemPool[K, V]) findIdx(reverse uint64) (int, error) {

	for i := range sp.items {
		if reverse <= sp.items[i].reverse {
			return i, nil
		}
	}
	return -1, ErrIdxOverflow

}

func (sp *samepleItemPool[K, V]) split(idx int) (nPool *samepleItemPool[K, V], err error) {
	return sp._split(idx, true)

}
func (sp *samepleItemPool[K, V]) _split(idx int, connect bool) (nPool *samepleItemPool[K, V], err error) {

	nlen := int64(len(sp.items))
	if int(nlen) <= idx {
		return nil, ErrIdxOverflow
	}

	nPool = &samepleItemPool[K, V]{reusable: sp.reusable}

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

func (sp *samepleItemPool[K, V]) initFreeList() {

	elist_head.InitAsEmpty(&sp.freeHead, &sp.freeTail)
}

func (sp *samepleItemPool[K, V]) PushWithOrder(item *SampleItem[K, V]) error {

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

func (sp *samepleItemPool[K, V]) shrinkLen() {

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
type itemSlice[K Key[K], V any] struct {
	sliceHeader
}

func sampleItemItemsOffset[K Key[K], V any]() uintptr {
	return unsafe.Offsetof(EmptysamepleItemPool[K, V]().items)
}
func itemSize[K Key[K], V any]() uintptr {
	return unsafe.Sizeof(SampleItem[K, V]{})
}

func toPtrItemSlice[K Key[K], V any](items *[]SampleItem[K, V]) (list *itemSlice[K, V]) {
	return (*itemSlice[K, V])(unsafe.Pointer(items))
}

func toItemSlice[K Key[K], V any](items []SampleItem[K, V]) (list itemSlice[K, V]) {
	slice := (*itemSlice[K, V])(unsafe.Pointer(&items))
	list.data = atomic.LoadPointer(&slice.data)
	list.len = atomic_util.LoadInt(&slice.len)
	list.cap = atomic_util.LoadInt(&slice.cap)
	return
}

func (sp *samepleItemPool[K, V]) ptrItems() (result *itemSlice[K, V]) {
	return (*itemSlice[K, V])(unsafe.Add(unsafe.Pointer(sp), sampleItemItemsOffset[K, V]()))
}

// publishItems replaces the array under the bucket lock. An odd publication
// means readers cannot yet pair its data pointer with its length.
func (sp *samepleItemPool[K, V]) publishItems(items *itemSlice[K, V]) {
	sp.publication.Add(1)
	sp.ptrItems().CopyFrom(items, 0, items.Len())
	sp.publication.Add(1)
}

func (sp *samepleItemPool[K, V]) itemSlice(isNoneZero bool) (result itemSlice[K, V]) {
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

func (list *itemSlice[K, V]) at(i int) (result *SampleItem[K, V]) {

	return list._at(i, true, false)
}

func (list *itemSlice[K, V]) _at(i int, checklen bool, skipOnDelete bool) (result *SampleItem[K, V]) {

	if checklen && atomic_util.LoadInt(&list.len) <= i {
		return nil
	} else if atomic_util.LoadInt(&list.cap) <= i {
		return nil
	}

	data := atomic.LoadPointer(&list.data)
	pCur := unsafe.Add(data, i*int(itemSize[K, V]()))
	if !skipOnDelete {
		return (*SampleItem[K, V])(pCur)
	}

	return skipDeletedItems[K, V]((*SampleItem[K, V])(pCur), (*SampleItem[K, V])(data))
}

func skipDeletedItems[K Key[K], V any](item, first *SampleItem[K, V]) *SampleItem[K, V] {
	for item.IsDeleted() && item != first {
		item = (*SampleItem[K, V])(unsafe.Add(unsafe.Pointer(item), -int(itemSize[K, V]())))
	}
	return item
}

func (list *itemSlice[K, V]) Len() int {

	return atomic_util.LoadInt(&list.len)
}

func (list *itemSlice[K, V]) Cap() int {

	return atomic_util.LoadInt(&list.cap)
}
func (list *itemSlice[K, V]) init() {
	atomic.StorePointer(&list.data, nil)
	atomic_util.StoreInt(&list.len, 0)
	atomic_util.StoreInt(&list.cap, 0)

}

//go:norace
func (list *itemSlice[K, V]) CopyFrom(slist *itemSlice[K, V], head, len int) {
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

func (list *itemSlice[K, V]) CopyDataFrom(head int, src *itemSlice[K, V], start, count int) {
	for i := 0; i < count; i++ {
		to, from := list._at(head+i, false, false), src._at(start+i, false, false)
		to.copyFrom(from)
	}
}

func (list *itemSlice[K, V]) dup() (new *itemSlice[K, V]) {
	new = &itemSlice[K, V]{}
	new.init()
	new.CopyFrom(list, 0, list.Len())
	return
}

func (list *itemSlice[K, V]) reverseAt(idx int) (r uint64) {

	var toReverse = unsafe.Offsetof(EmptySampleHMapEntry[K, V]().reverse)
	ptr := unsafe.Add(atomic.LoadPointer(&list.data), idx*int(SampleItemSize[K, V]())+int(toReverse))
	r = atomic.LoadUint64((*uint64)(ptr))
	return r
}

func (list *itemSlice[K, V]) reduceCap() int {

	return 0
}

func newPoolItems[K Key[K], V any](length, capacity int, reusable bool) []Entry[K, V] {
	items := make([]Entry[K, V], length, capacity)
	if reusable {
		for i := range items[:capacity] {
			items[:capacity][i].reusable = true
		}
	}
	return items
}
func copyPoolItems[K Key[K], V any](dst, src []Entry[K, V]) {
	for i := range src {
		dst[i].copyFrom(&src[i])
	}
}

func linkPoolItems[K Key[K], V any](items []Entry[K, V], exclude int) (first, last int) {
	first, last = -1, -1
	for i := range items {
		items[i].ListHead.Init()
		if i == exclude || items[i].IsIgnored() || !items[i].waitPayload() {
			continue
		}
		if first < 0 {
			first = i
		}
		last = i
	}
	if first == last {
		return
	}
	elist_head.InitAsEmpty(&items[first].ListHead, &items[last].ListHead)
	for i := first + 1; i < last; i++ {
		if i == exclude || items[i].IsIgnored() || !items[i].waitPayload() {
			continue
		}
		if _, err := items[last].ListHead.InsertBefore(&items[i].ListHead); err != nil {
			panic(err)
		}
	}
	return
}
