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

func (h *Map[K, V]) bsearchBybucket(bucket *bucket[K, V], reverseNoMask uint64, ignoreBucketEnry bool) *embeddedEntry[K, V] {
	entry, _, _ := h.bsearchPool(bucket, reverseNoMask, ignoreBucketEnry)
	return entry
}

// bsearchPool returns the array generation with the candidate so callers can
// validate a collision walk that overlaps retirement of the source array.
func (h *Map[K, V]) bsearchPool(bucket *bucket[K, V], reverseNoMask uint64, ignoreBucketEnry bool) (*embeddedEntry[K, V], *samepleItemPool[K, V], uint64) {

	stepAt("bsearch.begin", unsafe.Pointer(bucket), nil)
	pool := bucket.toBase().itemPool()
	// FIXME: why fail to get
	if pool == nil {
		pool = bucket.toBase().itemPool()
	}
	// MENTION: should remove 0 slice ?
	//items := pool.itemSlice(false)
	items := pool.ptrItems()
	var version uint64
	// insertToPool may put a new array into the pool while the search reads
	// the old one; the search starts again when the array changed under it
	for {
		version = pool.arrayState.Load()
		data := atomic.LoadPointer(&items.data)
		l := items.Len()
		stepAt("bsearch.snapshot", unsafe.Pointer(pool), nil)
		if version&1 != 0 || pool.arrayState.Load() != version {
			runtime.Gosched()
			continue
		}
		snapshot := unsafe.Slice((*embeddedEntry[K, V])(data), l)

		idx := sort.Search(l, func(i int) bool {
			item := skipDeletedItems[K, V](&snapshot[i], &snapshot[0], unsafe.Sizeof(embeddedEntry[K, V]{}))
			return atomic.LoadUint64(&item.reverse) >= reverseNoMask
			//return items.reverseAt(i) >= reverseNoMask
		})
		stepAt("bsearch.searched", unsafe.Pointer(bucket), nil)
		// Equal reverses sit next to each other; a purged placeholder may
		// precede the live entry of the same key, so skip ignored matches.
		var found *embeddedEntry[K, V]
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
		if found == nil {
			// External entries occupy the links between the binary-search anchors.
			start := bucket.toBase().head()
			for i := idx - 1; i >= 0; i-- {
				item := &snapshot[i]
				if atomic.LoadUint64(&item.reverse) < reverseNoMask && !item.IsIgnored() && linkedEntry(&item.ListHead) {
					start = &item.ListHead
					break
				}
			}
			retry := false
			for cur := start; cur != h.tail; cur = cur.DirectNext() {
				if cur.IsMarked() || cur.DirectNext() == cur {
					retry = true
					break
				}
				mh := mapheadFromLListHead(cur)
				reverse := atomic.LoadUint64(&mh.reverse)
				if reverse > reverseNoMask {
					break
				}
				if reverse == reverseNoMask && !mh.IsDummy() && (!ignoreBucketEnry || !mh.IsIgnored()) {
					found = entryHMapFromListHead[K, V](cur)
					break
				}
			}
			if retry {
				continue
			}
		}
		if pool.arrayState.Load() != version {
			continue
		}
		if found != nil {
			return found, pool, version
		}
		break
	}

	a := bucket.toBase().prevAsB()
	if a.reverse < reverseNoMask {
		if nb := h.findBucket(reverseNoMask); nb.toBase() != bucket.toBase() {
			return h.bsearchPool(nb, reverseNoMask, ignoreBucketEnry)
		}
	}

	return nil, pool, version
}

func (h *Map[K, V]) searchKeyFromEmbeddedPool(k uint64, ignoreBucketEnry bool) *embeddedEntry[K, V] {
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

	olen := bucket.itemPool().items.Len()
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

// bsearchFromFreeList finds a deleted slot without breaking hash order. Check
// the first larger slot, the equal-reverse run and its immediate predecessor.
// Replacements and suffix moves leave free slots inside the ordered array.
func (sp *samepleItemPool[K, V]) bsearchFromFreeList(reverse uint64) (int, bool) {

	items := sp.ptrItems()

	idx := sort.Search(items.Len(), func(i int) bool {
		item := items._at(i, true, false)
		return atomic.LoadUint64(&item.reverse) > reverse
	})
	if idx < items.Len() && items.at(idx).IsDeleted() {
		return idx, true
	}
	if idx < 1 {
		idx = 1
	}
	for i := idx - 1; i >= 0; i-- {
		item := items._at(i, true, false)
		if item != nil && item.IsDeleted() {
			return i, true
		}
		if item == nil || atomic.LoadUint64(&item.reverse) != reverse {
			break
		}
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
func (sp *samepleItemPool[K, V]) appendLast(reverse uint64, mu sync.Locker) (newItem *embeddedEntry[K, V], nPool *samepleItemPool[K, V], fn unlocker) {

	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	var new *embeddedEntry[K, V]
	items := sp.ptrItems()
	l := items.Len()
	if l >= items.Cap() {
		return nil, nil, fn
	}
	// the slot may be one released by purgeInEmbedded: clear its old
	// identity and give it reverse while it is past the length, where no
	// reader looks at it yet
	slot := items._at(l, false, false)
	if slot.isDetached() {
		slot.ListHead.Init()
	}
	atomic.AndUint64((*uint64)(&slot.PtrMapHead().state), ^uint64(mapIsDummy|mapIsDeleted|mapIsPoolItem|mapIsBusy|mapIsDetached))
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

// freeSlot reports whether slot j can take an entry: after the length a slot
// that newPoolItems made and nothing took, within it a deleted slot that is
// off the list, one that insertToPool detached with its block or one that a
// move copied without linking. A Delete without Purge leaves its slot on the
// list, so that slot is not free.
func (sp *samepleItemPool[K, V]) freeSlot(j, olen int) bool {
	item := sp.items._at(j, false, false)
	state := mapState(atomic.LoadUint64((*uint64)(&item.state)))
	if j >= olen {
		return state == mapIsReusable
	}
	return state&mapIsDeleted != 0 && (state&mapIsDetached != 0 || !linkedEntry(&item.ListHead))
}

// freeRunFor returns the slot that a new entry at i takes, the start of the
// first run of free slots after i that holds it and the block [i, blockEnd)
// of the slots after it, which goes behind it. The block ends at the first
// free slot after i; a run of free slots that is too short goes into the
// block with the entries after it, and the search goes on from the next run.
// After the length, a slot that is not fresh is passed over, not taken into
// the block. It returns -1 when no run is long enough before the capacity
// ends. The slots the block leaves stay holes, with their links, for a
// caller that still holds one of them: the new entry never takes them.
func (sp *samepleItemPool[K, V]) freeRunFor(i, olen, ocap int) (dst, blockEnd int) {
	blockEnd = i
	for {
		for blockEnd < olen && !sp.freeSlot(blockEnd, olen) {
			blockEnd++
		}
		need := blockEnd - i + 1
		start := blockEnd
		for {
			run := 0
			for start+run < ocap && sp.freeSlot(start+run, olen) {
				run++
			}
			if run >= need {
				return start, blockEnd
			}
			if start+run >= ocap {
				return -1, 0
			}
			if start+run < olen {
				blockEnd = start + run
				break
			}
			start += run + 1
		}
	}
}

// slideBlockToFreeRun puts the key of reverse at dst, the start of a free run
// at blockEnd or after it, and copies the block [i, blockEnd) of the slots
// behind it; the run holds both. The slots from i before dst stay as holes
// with the reverse of the new key, so that a binary search keeps its order
// and a key below the new one reuses them. The length grows when the copy
// reaches past it. It returns the slot of the new key.
func (sp *samepleItemPool[K, V]) slideBlockToFreeRun(reverse uint64, i, blockEnd, dst, olen int) *embeddedEntry[K, V] {
	need := blockEnd - i
	if EnableStats {
		if dst < olen {
			DebugStats[CntPoolHoleSlide].Add(1)
		} else {
			DebugStats[CntPoolSlide].Add(1)
		}
	}
	for m := dst; m <= dst+need && m < olen; m++ {
		sp.items.at(m).reclaimHole()
	}
	// Readers must retry throughout retirement, not only during publication.
	sp.arrayState.Add(1)
	switch {
	case need == 1:
		// the block of one needs no slices, which the call would allocate
		movePoolItem(sp.items._at(dst+1, false, false), sp.items._at(i, false, false))
	case need > 1:
		movePoolItemsInto(sp.items.slice(dst+1, dst+1+need), sp.items.slice(i, blockEnd), dst+1 < olen)
	}
	for m := dst + 1; m <= dst+need && m < olen; m++ {
		sp.items.at(m).releaseWrite()
	}
	// Keep the prefix in place. Retired source slots remain holes whose
	// hashes preserve the order used by searches and subsequent reuse.
	for j := i; j < dst; j++ {
		item := sp.items._at(j, false, false)
		atomic.OrUint64((*uint64)(&item.state), uint64(mapIsDeleted|mapIsRetired|mapIsDetached))
		atomic.StoreUint64(&item.reverse, reverse)
	}
	opened := sp.items._at(dst, false, false)
	atomic.StoreUint64(&opened.reverse, reverse)
	if dst < olen {
		opened.releaseWrite()
	}
	if end := dst + 1 + need; end > olen {
		newItems := sp.items.slice(0, end)
		stepAt("insertToPool.publish", unsafe.Pointer(sp), nil)
		sp.ptrItems().CopyFrom(&newItems, 0, newItems.Len())
	}
	sp.arrayState.Add(1)
	return opened
}

// freeRunBefore is freeRunFor towards the start of the slots: it returns the
// start of the first run of free slots before i that holds the block
// [blockStart, i) of the slots before i and, after it, the new entry. The
// block starts after the first free slot before i; a run that is too short
// goes into the block with the entries before it. It returns -1 when no run
// is long enough before the start.
func (sp *samepleItemPool[K, V]) freeRunBefore(i, olen int) (dst, blockStart int) {
	blockStart = i
	for {
		for blockStart > 0 && !sp.freeSlot(blockStart-1, olen) {
			blockStart--
		}
		need := i - blockStart + 1
		run := 0
		for blockStart-run > 0 && sp.freeSlot(blockStart-run-1, olen) {
			run++
		}
		if run >= need {
			return blockStart - run, blockStart
		}
		if blockStart-run <= 0 {
			return -1, 0
		}
		blockStart -= run
	}
}

// slideBlockToFreeRunBefore copies the block [blockStart, i) of the slots to
// the free run at dst before it and puts the key of reverse after the copy;
// the run holds both. The slots after the new key before i stay as holes
// with the reverse of the entry at i, so that a binary search keeps its
// order and a key between the new one and that entry reuses them. It
// returns the slot of the new key.
func (sp *samepleItemPool[K, V]) slideBlockToFreeRunBefore(reverse uint64, blockStart, i, dst int) *embeddedEntry[K, V] {
	need := i - blockStart
	if EnableStats {
		DebugStats[CntPoolHoleSlide].Add(1)
	}
	for m := dst; m <= dst+need; m++ {
		sp.items.at(m).reclaimHole()
	}
	// Readers must retry throughout retirement, not only during publication.
	sp.arrayState.Add(1)
	switch {
	case need == 1:
		movePoolItem(sp.items._at(dst, false, false), sp.items._at(blockStart, false, false))
	case need > 1:
		movePoolItemsInto(sp.items.slice(dst, dst+need), sp.items.slice(blockStart, i), true)
	}
	for m := dst; m < dst+need; m++ {
		sp.items.at(m).releaseWrite()
	}
	nextReverse := atomic.LoadUint64(&sp.items._at(i, false, false).reverse)
	for j := dst + need + 1; j < i; j++ {
		item := sp.items._at(j, false, false)
		atomic.OrUint64((*uint64)(&item.state), uint64(mapIsDeleted|mapIsRetired|mapIsDetached))
		atomic.StoreUint64(&item.reverse, nextReverse)
	}
	opened := sp.items._at(dst+need, false, false)
	atomic.StoreUint64(&opened.reverse, reverse)
	opened.releaseWrite()
	sp.arrayState.Add(1)
	return opened
}

// reclaimHole gives a free slot within the length the state of a slot that
// newPoolItems made and never handed out, so that it takes an entry the way
// the slots after the length do, and keeps it held for writing: a reader
// that still holds the slot from before waits until releaseWrite, after the
// entry is in. The payload version stays, so that reader sees the change.
func (e *embeddedEntry[K, V]) reclaimHole() {
	e.acquireWrite()
	state := atomic.LoadUint64((*uint64)(&e.state))
	e.PtrListHead().Init()
	atomic.StoreUint64(&e.conflict, 0)
	atomic.StoreUint64((*uint64)(&e.state), (state&^uint64(mapKeyWriting-1))|uint64(mapIsReusable|mapReadersMask))
}

func (sp *samepleItemPool[K, V]) insertToPool(reverse uint64, mu sync.Locker) (newItem *embeddedEntry[K, V], nPool *samepleItemPool[K, V], fn unlocker) {
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := sp.items.Len()
	ocap := sp.items.Cap()

	// require insert
	if IsDebug() {
		var b strings.Builder
		for i := 0; i < sp.items.Len(); i++ {
			sp.items.at(i).PtrMapHead().dump[K, V](&b)
		}
		Log(LogDebug, "B: itemPool.items\n%s\n", b.String())
	}
	// FIXME: should disable not IsDebug()
	//sp.validateItems()
	if olen != sp.items.Len() {
		Log(LogDebug, "update olen")
		newItem, nPool, _ = sp.getWithFn(reverse, nil)
		return newItem, nPool, fn
	}
	//olen = sp.items.Len()
	nlen := int64(olen)

	for i := 0; i < olen; i++ {
		//for i := 0; i < sp.items.Len(); i++ {
		if sp.items.at(i).reverse < reverse {
			continue
		}
		if i == olen-1 {
			//fmt.Printf("invalid")
		}
		if !atomic.CompareAndSwapInt64(&nlen, int64(sp.items.Len()), nlen+1) {
			Log(LogDebug, "update olen")
			newItem, nPool, _ = sp.getWithFn(reverse, nil)
			return newItem, nPool, fn
		}
		if dst, blockEnd := sp.freeRunFor(i, olen, ocap); dst >= 0 {
			return sp.slideBlockToFreeRun(reverse, i, blockEnd, dst, olen), nil, lazyUnlock
		}
		if dst, blockStart := sp.freeRunBefore(i, olen); dst >= 0 {
			return sp.slideBlockToFreeRunBefore(reverse, blockStart, i, dst), nil, lazyUnlock
		}
		newItems := newPoolItems[K, V](olen+1, maxInts(ocap, olen+1), true)
		if EnableStats {
			DebugStats[CntPoolInsertAlloc].Add(1)
		}
		insertAt := i
		// Readers must retry throughout retirement, not only during publication.
		sp.arrayState.Add(1)
		if i > 0 {
			movePoolItems(newItems.slice(0, i), sp.items.slice(0, i))
		}
		movePoolItems(newItems.slice(insertAt+1, newItems.Len()), sp.items.slice(i, olen))

		newItems.at(insertAt).PtrMapHead().reverse = reverse

		oldItems := sp.ptrItems().dup()
		newItemSlice := newItems
		stepAt("insertToPool.publish", unsafe.Pointer(sp), nil)
		sp.ptrItems().CopyFrom(&newItemSlice, 0, newItemSlice.Len())
		sp.arrayState.Add(1)

		// for debug
		_ = oldItems

		if IsDebug() {
			var b strings.Builder
			for i := 0; i < sp.items.Len(); i++ {
				sp.items.at(i).PtrMapHead().dump[K, V](&b)
			}
			fmt.Printf("A: itemPool.items\n%s\n", b.String())
		}

		return sp.items.at(insertAt), nil, lazyUnlock
	}
	return sp.getWithFn(reverse, nil)
	//return nil, nil, nil

}

//go:norace
func (sp *samepleItemPool[K, V]) getWithFn(reverse uint64, mu sync.Locker) (new *embeddedEntry[K, V], nPool *samepleItemPool[K, V], fn unlocker) {

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
		items := sp.ptrItems()
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
		if oState&uint64(mapIsDetached) != 0 {
			new.PtrListHead().Init()
			atomic.AndUint64((*uint64)(&new.state), ^uint64(mapIsDetached))
		} else if !new.PtrListHead().IsSingle() {
			new.PtrListHead().MarkForDelete()
		}
		return new, nil, fn
	}

	return sp.insertToPool(reverse, mu)

RETRY:
	if fn != nil {
		mu.Unlock()
	}
	return sp.getWithFn(reverse, mu)

}

func (sp *samepleItemPool[K, V]) expand(mu sync.Locker) (unlocker, error) {
	var fn unlocker
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := sp.items.Len()
	//ocap := sp.items.Cap()

	if olen != sp.items.Len() {
		Log(LogDebug, "update olen")
		_, e := sp.expand(nil)
		return fn, e
	}
	nlen := int64(olen)

	if !atomic.CompareAndSwapInt64(&nlen, int64(sp.items.Len()), nlen+1) {
		Log(LogDebug, "update olen")
		_, e := sp.expand(nil)
		return fn, e
	}

	nCap := PoolCap(sp.items.Len())

	newItems := newPoolItems[K, V](olen, nCap, true)
	newItems.CopyDataFrom(0, sp.ptrItems(), 0, olen)

	replacePoolItems(newItems, sp.items, -1)

	oldItems := sp.ptrItems().dup()
	sp.publishItems(&newItems)

	// for debug
	_ = oldItems

	if IsDebug() {
		var b strings.Builder
		for i := 0; i < sp.items.Len(); i++ {
			sp.items.at(i).PtrMapHead().dump[K, V](&b)
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

	for i := 0; i < sp.items.Len(); i++ {
		if reverse <= sp.items.at(i).reverse {
			return i, nil
		}
	}
	return -1, ErrIdxOverflow

}

func (sp *samepleItemPool[K, V]) split(idx int) (nPool *samepleItemPool[K, V], err error) {
	return sp._split(idx, true)

}
func (sp *samepleItemPool[K, V]) _split(idx int, connect bool) (nPool *samepleItemPool[K, V], err error) {

	nlen := int64(sp.items.Len())
	if int(nlen) <= idx {
		return nil, ErrIdxOverflow
	}

	nPool = &samepleItemPool[K, V]{reusable: sp.reusable}

	if !atomic.CompareAndSwapInt64(&nlen, int64(sp.items.Len()), nlen+1) {
		return sp._split(idx, connect)
	}

	// sp keeps its items: the caller shrinks sp after nPool's bucket is readable.
	spItems := sp.ptrItems()
	nPool.items.stride = spItems.stride
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

func (sp *samepleItemPool[K, V]) PushWithOrder(item *embeddedEntry[K, V]) error {

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
		if !item.isDetached() && !item.Empty() {
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
	stride uintptr
}

func (sp *samepleItemPool[K, V]) ptrItems() *itemSlice[K, V] {
	return &sp.items
}

// publishItems replaces the array under the bucket lock. An odd publication
// means readers cannot yet pair its data pointer with its length.
func (sp *samepleItemPool[K, V]) publishItems(items *itemSlice[K, V]) {
	sp.arrayState.Add(1)
	sp.ptrItems().CopyFrom(items, 0, items.Len())
	sp.arrayState.Add(1)
}

func (sp *samepleItemPool[K, V]) itemSlice(isNoneZero bool) (result itemSlice[K, V]) {
	for {
		//result = *sp.ptrItems()
		ptr := sp.ptrItems()
		result.data = atomic.LoadPointer(&ptr.data)
		result.len = atomic_util.LoadInt(&ptr.len)
		result.cap = atomic_util.LoadInt(&ptr.cap)
		result.stride = ptr.stride

		if result.data != nil {
			break
		}
		if !isNoneZero && result.Len()+result.Cap() == 0 {
			break
		}
	}
	return
}

func (list *itemSlice[K, V]) at(i int) (result *embeddedEntry[K, V]) {

	return list._at(i, true, false)
}

func (list *itemSlice[K, V]) _at(i int, checklen bool, skipOnDelete bool) (result *embeddedEntry[K, V]) {

	if checklen && atomic_util.LoadInt(&list.len) <= i {
		return nil
	} else if atomic_util.LoadInt(&list.cap) <= i {
		return nil
	}

	data := atomic.LoadPointer(&list.data)
	pCur := unsafe.Add(data, i*int(list.stride))
	if !skipOnDelete {
		return (*embeddedEntry[K, V])(pCur)
	}

	return skipDeletedItems[K, V]((*embeddedEntry[K, V])(pCur), (*embeddedEntry[K, V])(data), list.stride)
}

func skipDeletedItems[K Key[K], V any](item, first *embeddedEntry[K, V], stride uintptr) *embeddedEntry[K, V] {
	for item.IsDeleted() && item != first {
		item = (*embeddedEntry[K, V])(unsafe.Add(unsafe.Pointer(item), -int(stride)))
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
	new = &itemSlice[K, V]{stride: list.stride}
	new.init()
	new.CopyFrom(list, 0, list.Len())
	return
}

func (list *itemSlice[K, V]) reverseAt(idx int) (r uint64) {

	var toReverse = unsafe.Offsetof(embeddedEntry[K, V]{}.reverse)
	ptr := unsafe.Add(atomic.LoadPointer(&list.data), idx*int(list.stride)+int(toReverse))
	r = atomic.LoadUint64((*uint64)(ptr))
	return r
}

func (list *itemSlice[K, V]) reduceCap() int {

	return 0
}

func newPoolItems[K Key[K], V any](length, capacity int, reusable bool) itemSlice[K, V] {
	var data unsafe.Pointer
	stride := unsafe.Sizeof(Entry[K, V]{})
	if reusable {
		stride = unsafe.Sizeof(embeddedEntry[K, V]{})
		items := make([]embeddedEntry[K, V], capacity)
		for i := range items {
			items[i].state = mapIsReusable
		}
		data = unsafe.Pointer(unsafe.SliceData(items))
	} else {
		items := make([]Entry[K, V], capacity)
		data = unsafe.Pointer(unsafe.SliceData(items))
	}
	return itemSlice[K, V]{sliceHeader: sliceHeader{data: data, len: length, cap: capacity}, stride: stride}
}

func (list *itemSlice[K, V]) slice(start, end int) itemSlice[K, V] {
	return itemSlice[K, V]{
		sliceHeader: sliceHeader{
			data: unsafe.Add(atomic.LoadPointer(&list.data), uintptr(start)*list.stride),
			len:  end - start,
			cap:  list.Cap() - start,
		},
		stride: list.stride,
	}
}

// movePoolItems runs under the pool's writer lock. Neighbouring pools can
// change the links at a run's ends, so only its interior is copied as a slice.
func movePoolItems[K Key[K], V any](dst, src itemSlice[K, V]) {
	movePoolItemsInto(dst, src, false)
}

// copyListLinks copies the two relative links of src into dst with atomic
// stores: what a copy of the memory of an entry does for its ListHead, for a
// slot that a search can read.
func copyListLinks(dst, src *elist_head.ListHead) {
	const _ = uint(unsafe.Sizeof(elist_head.ListHead{}) - 2*unsafe.Sizeof(uintptr(0)))
	d := (*[2]uintptr)(unsafe.Pointer(dst))
	s := (*[2]uintptr)(unsafe.Pointer(src))
	atomic.StoreUintptr(&d[0], atomic.LoadUintptr(&s[0]))
	atomic.StoreUintptr(&d[1], atomic.LoadUintptr(&s[1]))
}

// movePoolItemsInto is movePoolItems; with published, the slots of dst are
// within the length, which a search can read, so each entry goes in with the
// stores of copyFrom, not with a copy of the memory of the block.
// movePoolItem moves the entry of src into dst, a slot that nothing uses yet:
// the block of one of movePoolItems, without the slices. A deleted or unready
// src leaves dst deleted and is taken off the list when it is still on it.
func movePoolItem[K Key[K], V any](dst, src *embeddedEntry[K, V]) {
	dst.copyFrom(src)
	if src.IsIgnored() || !src.waitPayload() {
		dst.Delete()
		if !src.isDetached() && linkedEntry(&src.ListHead) {
			src.ListHead.MarkForDelete()
		}
		return
	}
	old := elist_head.Block{First: &src.ListHead, Last: &src.ListHead}
	fresh := elist_head.Block{First: &dst.ListHead, Last: &dst.ListHead}
	fresh.InitCopiedFrom(old)
	if linkedEntry(old.First) {
		for fresh.InsertBefore(old.First) != nil {
			runtime.Gosched()
		}
		stepAt("copy.poolblock.inserted", unsafe.Pointer(fresh.First), unsafe.Pointer(old.First))
		if err := old.Delete(); err != nil {
			panic(fmt.Sprintf("movePoolItem: detach source block: %v", err))
		}
	}
	src.Delete()
}

func movePoolItemsInto[K Key[K], V any](dst, src itemSlice[K, V], published bool) {
	for first := 0; first < src.Len(); {
		if src.at(first).IsIgnored() || !src.at(first).waitPayload() {
			movePoolItem(dst.at(first), src.at(first))
			first++
			continue
		}
		last := first
		for last+1 < src.Len() && !src.at(last+1).IsIgnored() && src.at(last).ListHead.DirectNext() == &src.at(last+1).ListHead &&
			src.at(last+1).ListHead.DirectPrev() == &src.at(last).ListHead {
			last++
		}
		if last == first {
			movePoolItem(dst.at(first), src.at(first))
			first++
			continue
		}
		dst.at(first).copyFrom(src.at(first))
		dst.at(last).copyFrom(src.at(last))
		if published {
			// InitCopiedFrom needs the interior links copied with the same
			// layout, as the copy of the memory does
			for i := first + 1; i < last; i++ {
				dst.at(i).copyFrom(src.at(i))
				copyListLinks(&dst.at(i).ListHead, &src.at(i).ListHead)
			}
		} else {
			for i := first + 1; i < last; i++ {
				src.at(i).acquireWrite()
			}
			if last > first+1 {
				copy(unsafe.Slice(dst.at(first+1), last-first-1), unsafe.Slice(src.at(first+1), last-first-1))
			}
			for i := first + 1; i < last; i++ {
				dst.at(i).state &= mapIsDummy | mapIsDeleted | mapIsPoolItem | mapPayloadReady | mapIsReusable
				src.at(i).releaseWrite()
			}
		}
		old := elist_head.Block{First: &src.at(first).ListHead, Last: &src.at(last).ListHead}
		fresh := elist_head.Block{First: &dst.at(first).ListHead, Last: &dst.at(last).ListHead}
		fresh.InitCopiedFrom(old)
		if linkedEntry(old.First) {
			for fresh.InsertBefore(old.First) != nil {
				runtime.Gosched()
			}
			stepAt("copy.poolblock.inserted", unsafe.Pointer(fresh.First), unsafe.Pointer(old.First))
			if err := old.Delete(); err != nil {
				panic(fmt.Sprintf("movePoolItems: detach source block: %v", err))
			}
		}
		for i := first; i <= last; i++ {
			src.at(i).Delete()
		}
		first = last + 1
	}
}

func replacePoolItems[K Key[K], V any](dst, src itemSlice[K, V], exclude int) {
	for i := 0; i < src.Len(); i++ {
		j := i
		if exclude >= 0 && i >= exclude {
			j++
		}
		dst.at(j).ListHead.Init()
		if src.at(i).IsIgnored() || !src.at(i).waitPayload() {
			dst.at(j).Delete()
			if !src.at(i).isDetached() && linkedEntry(&src.at(i).ListHead) {
				src.at(i).ListHead.MarkForDelete()
			}
			continue
		}
		_, moved, published := replaceEntryListNode(src.at(i), dst.at(j), "copy.poolmove.inserted")
		if !published {
			moved.Delete()
		}
	}
}
