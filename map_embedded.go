// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
)

func (h *Map) bsearchBybucket(bucket *bucket, reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry {

	pool := bucket.toBase().itemPool()
	// FIXME: why fail to get
	if pool == nil {
		pool = bucket.toBase().itemPool()
	}

	idx := sort.Search(len(pool.items), func(i int) bool {
		return pool.items[i].reverse >= reverseNoMask
	})
	if idx < len(pool.items) && pool.items[idx].reverse == reverseNoMask {
		return &pool.items[idx]
	}

	a := bucket.toBase().prevAsB()
	_ = a
	if a.reverse < reverseNoMask {
		h.findBucket(reverseNoMask)
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
	b := h.bucketFromPoolEmbedded(newReverse)
	if b == nil {
		return ErrBucketAllocatedFail
	}

	if b.reverse == 0 && b.level > 1 {
		err = NewError(EBucketInvalid, "bucket.reverse = 0. but level 1= 1", nil)
		Log(LogWarn, err.Error())
		return
	}
	b.Init()
	b.LevelHead.Init()
	b.initItemPool()

	idx, err := bucket.itemPool().findIdx(newReverse)
	if err != nil || idx == 0 {
		return err
	}

	olen := len(bucket.itemPool().items)
	nPool, err2 := bucket.itemPool()._split(idx, false)
	_ = err2
	b.setItemPool(nPool)
	atomic.StoreInt32(&bucket._len, int32(idx))
	atomic.StoreInt32(&b._len, int32(olen-idx))

	h.addBucket(b)

	nextLevel := h.findNextLevelBucket(b.reverse, b.level)

	if b.LevelHead.DirectNext() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	if nextLevel != nil {

		nextLevelBucket := bucketFromLevelHead(nextLevel)
		if nextLevelBucket.reverse < b.reverse {
			nextLevel.InsertBefore(&b.LevelHead)
		} else if nextLevelBucket.reverse != b.reverse {

			nextnextBucket := bucketFromLevelHead(nextLevel.Next())
			_ = nextnextBucket
			nextLevel.DirectNext().InsertBefore(&b.LevelHead)
		}

	} else {
		Log(LogWarn, "not found level bucket.")
	}
	if b.LevelHead.Next() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	if int(b.len()) > h.maxPerBucket {
		h.makeBucket2(b)
	} else if int(bucket.len()) > h.maxPerBucket {
		h.makeBucket2(bucket)
	}

	return nil
}

func (b *bucket) toBase() *bucket {

	if b._parent == nil {
		return b
	}
	return b._parent.toBase()
}

func (h *Map) bucketFromPoolEmbedded(reverse uint64) (b *bucket) {

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
		if cap(b.downLevels) == 0 {
			b.downLevels = make([]bucket, 1, 16)
			b.downLevels[0].level = b.level + 1
			b.downLevels[0].reverse = b.reverse
			b.downLevels[0].Init()
			b.downLevels[0].LevelHead.Init()
			b.downLevels[0]._parent = b

			b.downLevels[0].setItemPoolFn = func(p *samepleItemPool) {
				b.setItemPool(p)
			}

			lCur := h.levelBucket(l)
			if lCur.LevelHead.Empty() {
				lCur = bucketFromLevelHead(lCur.LevelHead.DirectPrev().DirectNext())
			}
			for ; lCur != lCur.NextOnLevel(); lCur = lCur.NextOnLevel() {
				if lCur.LevelHead.Empty() {
					break
				}
				if lCur.reverse < b.reverse {
					break
				}
			}
			lCur.LevelHead.InsertBefore(&b.downLevels[0].LevelHead)
		}
		if len(b.downLevels) <= idx {
			b.downLevels = b.downLevels[:idx+1]
		}

		if b.downLevels[idx].level == 0 {
			if l != level {
				Log(LogWarn, "not collected already inited")
			}
			b.downLevels[idx].level = b.level + 1
			b.downLevels[idx].reverse = b.reverse | (uint64(idx) << (4 * (16 - l)))
			b = &b.downLevels[idx]
			if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
				Log(LogWarn, "already inited")
			}
			break
		}
		b = &b.downLevels[idx]
	}
	if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
		h.DumpBucket(logio)
		Log(LogWarn, "already inited")
	}
	return

}

const (
	getEmpty      byte = 1
	getLargest         = 2
	getNoCap           = 3
	requireInsert      = 4
)

func (sp *samepleItemPool) state4get(reverse uint64, tail int) byte {

	if len(sp.items) == 0 {
		return getEmpty
	}
	if cap(sp.items) == len(sp.items) {
		return getNoCap
	}

	last := &sp.items[tail]
	if last.reverse < reverse {
		return getLargest
	}
	return requireInsert

}

var lastgets []byte = nil

func lazyUnlock(mu *sync.Mutex) {
	if mu != nil {
		mu.Unlock()
	}
}

type unlocker func(mu *sync.Mutex)

func (sp *samepleItemPool) appendLast(mu *sync.Mutex) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {

	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	sp.items = sp.items[:olen+1]
	new := &sp.items[olen]
	new.Init()
	return new, nil, fn

}

func (sp *samepleItemPool) insertToPool(reverse uint64, mu *sync.Mutex) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	ocap := cap(sp.items)

	// require insert
	//FIXME: should bsearch
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
		return sp.getWithFn(reverse, nil)
	}
	//olen = len(sp.items)
	nlen := int64(olen)

	for i := 0; i < olen; i++ {
		//for i := range sp.items {
		if sp.items[i].IsIgnored() {
			continue
		}
		if sp.items[i].reverse < reverse {
			continue
		}
		if i == olen-1 {
			//fmt.Printf("invalid")
		}
		if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
			Log(LogDebug, "update olen")
			return sp.getWithFn(reverse, nil)
		}
		var err error
		prevItem := sp.items[0].ListHead.Prev()
		nextItem := sp.items[olen-1].ListHead.Next()

		// copy to new slice
		newItems := make([]SampleItem, olen+1, maxInts(ocap, olen+1))
		if i > 0 {
			copy(newItems[0:i], sp.items[0:i])
		}
		copy(newItems[i+1:], sp.items[i:])

		newListHead := &elist_head.ListHead{}
		newListTail := &elist_head.ListHead{}
		elist_head.InitAsEmpty(newListHead, newListTail)

		middle := nextListHeadOfSampleItem()
		for i := 0; i < olen+1; i++ {
			newItems[i].ListHead = middle
		}

		err = newListHead.ReplaceNext(&newItems[0].ListHead, &newItems[olen].ListHead, newListTail)
		if err != nil {
			Log(LogFatal, "replace fail")
		}
		first := EmptyMapHead.fromListHead(newListHead.Next())
		last := EmptyMapHead.fromListHead(newListTail.Prev())
		pOpts := elist_head.SharedTrav(list_head.WaitNoM())
		newItems[i].MarkForDelete()
		elist_head.SharedTrav(pOpts...)
		newItems[i].Init()

		// for debug
		first = EmptyMapHead.fromListHead(newListHead.Next())
		last = EmptyMapHead.fromListHead(newListTail.Next())
		_, _ = first, last

		err = prevItem.ReplaceNext(newListHead.Next(), newListTail.Prev(), nextItem)
		if err != nil {
			Log(LogFatal, "fail to replace newItems")
		}
		oldItems := sp.items
		sp.items = newItems

		// for debug
		oldItemFirst := oldItems[0].Prev().Next().PtrMapHead()
		oldItemNext := oldItems[olen-1].Next().PtrMapHead()
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

func (sp *samepleItemPool) getWithFn(reverse uint64, mu *sync.Mutex) (new MapItem, nPool *samepleItemPool, fn unlocker) {

	lastActiveIdx := -1
	olen := len(sp.items)

	for i := olen - 1; i >= 0; i-- {
		if sp.items[i].IsIgnored() {
			continue
		}
		lastActiveIdx = i
		break
	}

	defer func() {
		if new == nil {
			Log(LogWarn, "getWithFn(): item is nil")
		}

	}()

	// for debug
	//lastgets = append(lastgets, sp.state4get(reverse, lastActiveIdx))
	switch sp.state4get(reverse, lastActiveIdx) {
	case getEmpty, getLargest:
		return sp.appendLast(mu)
	case getNoCap:
		fn, err := sp.expand(mu)
		if err != nil {
			Log(LogWarn, "pool.expand() require retry")
		}
		new, nPool, _ = sp.getWithFn(reverse, nil)
		return new, nPool, fn
	}

	return sp.insertToPool(reverse, mu)

}

func (sp *samepleItemPool) expand(mu *sync.Mutex) (unlocker, error) {
	var fn unlocker
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	//ocap := cap(sp.items)

	if olen != len(sp.items) {
		Log(LogDebug, "update olen")
		return sp.expand(nil)
	}
	nlen := int64(olen)

	if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
		Log(LogDebug, "update olen")
		return sp.expand(nil)
	}

	var err error
	prevItem := sp.items[0].ListHead.Prev()
	nextItem := sp.items[olen-1].ListHead.Next()

	nCap := 0
	if len(sp.items) < 128 {
		nCap = 256
	} else {
		nCap = calcCap(len(sp.items))
	}

	newItems := make([]SampleItem, olen, nCap)
	copy(newItems, sp.items[0:olen])

	newListHead := &elist_head.ListHead{}
	newListTail := &elist_head.ListHead{}
	elist_head.InitAsEmpty(newListHead, newListTail)

	err = newListHead.ReplaceNext(&newItems[0].ListHead, &newItems[olen-1].ListHead, newListTail)
	if err != nil {
		Log(LogFatal, "replace fail")
	}

	err = prevItem.ReplaceNext(newListHead.Next(), newListTail.Prev(), nextItem)
	if err != nil {
		Log(LogFatal, "fail to replace newItems")
	}
	oldItems := sp.items
	sp.items = newItems

	// for debug
	oldItemFirst := oldItems[0].Prev().Next().PtrMapHead()
	oldItemNext := oldItems[olen-1].Next().PtrMapHead()
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

func nextListHeadOfSampleItem() elist_head.ListHead {

	list := elist_head.NewEmptyList()

	items := make([]SampleItem, 3)

	list.Tail().InsertBefore(items[2].PtrListHead())
	items[2].InsertBefore(items[1].PtrListHead())
	items[1].InsertBefore(items[0].PtrListHead())

	return items[1].ListHead
}

func (sp *samepleItemPool) findIdx(reverse uint64) (int, error) {

	for i := range sp.items {
		if sp.items[i].IsIgnored() {
			continue
		}
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
	nPool.items = sp.items[idx:]
	sp.items = sp.items[:idx:idx]
	nPool.Init()
	//sp.validateItems()
	//nPool.validateItems()

	if connect && sp.PtrListHead().Next() != nil && sp.PtrListHead().Next() != sp.PtrListHead() {
		_, err = sp.PtrListHead().Next().InsertBefore(nPool.PtrListHead())
	}
	return

}
