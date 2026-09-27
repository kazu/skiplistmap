// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"fmt"
	"io"
	"math/bits"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap/atomic_util"
	"github.com/lk4d4/trylock"
)

//const cntOfHampBucket = 32

type SearchMode byte

const (
	LenearSearchForBucket SearchMode = iota
	NestedSearchForBucket
	CombineSearch
	CombineSearch2
	CombineSearch3
	CombineSearch4

	NoItemSearchForBucket = 9  // test mode
	FalsesSearchForBucket = 10 // test mode
)

// Map ... Skip List Map is an ordered and concurrent map.
// this Map is gourtine safety for reading/updating/deleting, require locking and coordination. This
type Map struct {
	buckets    [16]bucket
	headBucket *list_head.ListHead
	tailBucket *list_head.ListHead

	len          int64
	maxPerBucket int
	head         *elist_head.ListHead
	tail         *elist_head.ListHead

	modeForBucket SearchMode
	mu            sync.Mutex
	levels        [16]atomic.Value

	ItemFn func() MapItem

	pooler *Pool

	isEmbededItemInBucket bool
}

var conf mapConf = mapConf{
	minCapItems:  2,
	thresholdCap: 256,
}

type mapConf struct {
	minCapItems  int
	thresholdCap int
}

func minCapItem() int {
	return atomic_util.LoadInt(&conf.minCapItems)
}

func thresholdCapItem() int {
	return atomic_util.LoadInt(&conf.thresholdCap)
}

type LevelHead list_head.ListHead

// 	eBuf       []entryBuffer
// 	maxPefEbuf int
// }

//type LevelHead list_head.ListHead

type OptHMap func(*Map) OptHMap

func MaxPefBucket(max int) OptHMap {

	return func(h *Map) OptHMap {
		prev := h.maxPerBucket
		h.maxPerBucket = max
		return MaxPefBucket(prev)
	}
}

func BucketMode(mode SearchMode) OptHMap {
	return func(h *Map) OptHMap {
		prev := h.modeForBucket
		h.modeForBucket = mode
		return BucketMode(prev)
	}
}

func ItemFn(fn func() MapItem) OptHMap {
	return func(h *Map) OptHMap {
		prev := h.ItemFn
		h.ItemFn = fn
		if prev == nil {
			return nil
		}
		return ItemFn(prev)
	}
}

func UsePool(enable bool) OptHMap {
	return func(h *Map) OptHMap {
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&h.pooler))
		if !enable {
			h.pooler = nil
		} else if atomic.LoadPointer(ptr) == nil {
			p := newPool()
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(p)) {
				p.startMgr()
			}
		}

		return UsePool(!enable)
	}
}

func UseEmbeddedPool(enable bool) OptHMap {
	return func(h *Map) OptHMap {
		old := h.isEmbededItemInBucket
		h.isEmbededItemInBucket = enable
		if enable {
			UsePool(false)(h)
		}

		return UsePool(old)
	}
}

func (h *Map) Options(opts ...OptHMap) (previouses []OptHMap) {

	for _, fn := range opts {
		previouses = append(previouses, fn(h))
	}
	return

}
func New(opts ...OptHMap) *Map {

	return NewHMap(opts...)

}

func NewHMap(opts ...OptHMap) *Map {
	hmap := &Map{
		len:          0,
		maxPerBucket: 32,
	}

	topBucket := newBucket()
	topBucket.InitAsEmpty()
	hmap.tailBucket = topBucket.Next()
	hmap.headBucket = topBucket.Prev()

	hmap.head = &elist_head.ListHead{}
	hmap.tail = &elist_head.ListHead{}
	elist_head.InitAsEmpty(hmap.head, hmap.tail)

	hmap.modeForBucket = NestedSearchForBucket
	hmap.ItemFn = func() MapItem { return emptyEntryHMap }
	hmap.isEmbededItemInBucket = false

	hmap.Options(opts...)

	hmap.initLevels()

	hmap.initBeforeSet()

	// FIXME: remove later
	//hmap.cactchSigBua()

	return hmap
}

func (h *Map) cactchSigBua() {

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGBUS, syscall.SIGSEGV)

		s := <-sig
		_ = s
		fmt.Printf("panic?\n")

		var b strings.Builder
		h.DumpBucket(&b)
		h.DumpEntry(&b)
		fmt.Println(b.String())
		fmt.Printf("panic?\n")

	}()
}

func (h *Map) Len() int {
	return int(h.len)
}

func (h *Map) AddLen(inc int64) int64 {
	return atomic.AddInt64(&h.len, inc)
}

func (h *Map) initBeforeSet() {
	if !h.notHaveBuckets() {
		return
	}
	btable := newBucket()
	btable._level, btable._len = 16, 0
	btable.reverse = ^uint64(0)
	btable.Init()
	btable.LevelHead.Init()

	empty := &btable.dummy
	empty.key, empty.value = nil, nil
	empty.reverse, empty.conflict = btable.reverse, 0
	empty.PtrMapHead().state |= mapIsDummy

	h.tail.InsertBefore(empty.PtrListHead())

	// add bucket
	h.tailBucket.InsertBefore(&btable.ListHead)

	levelBucket := h.levelBucket(btable._level)
	levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
	h.setLevel(btable._level, levelBucket)
	btable.state = bucketStateActive
	btablefirst := btable

	topReverses := make([]uint64, 16)

	for k := uint64(0); k < 16; k++ {
		topReverses[int(k)] = bits.Reverse64(k)
	}
	sort.Slice(topReverses, func(i, j int) bool { return topReverses[i] < topReverses[j] })

	for i := range topReverses {
		reverse := topReverses[i]
		btable = &h.buckets[i]
		btable._level, btable._len, btable.reverse = 1, 0, reverse
		btable.state = bucketStateInit
		btable.Init()
		btable.LevelHead.Init()

		if h.isEmbededItemInBucket {
			btable.initItemPool()
			if btable._itemPool == nil {
				btable._itemPool = &samepleItemPool{}
				btable._itemPool.Init()
			}
			btable.setupPool()
			btable._itemPool._init(h.maxPerBucket * 3 / 2)
		}

		empty = &btable.dummy
		empty.key, empty.value = nil, nil
		empty.reverse, empty.conflict = btable.reverse, 0
		empty.PtrMapHead().state |= mapIsDummy

		//inserBeforeWithCheck(btablefirst.head(), &empty.ListHead)
		btablefirst.head().InsertBefore(&empty.ListHead)

		// add bucket
		btablefirst.Next().InsertBefore(&btable.ListHead)
		if IsDebug() {
			h.validateBucket(btable)
		}

		btable.LevelHead.Init()
		if i > 0 {
			h.buckets[i-1].LevelHead.InsertBefore(&btable.LevelHead)
		} else {
			levelBucket = h.levelBucket(btable._level)
			levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
		}
		btable.state = bucketStateActive
	}

	if EnableStats {
		old := logio
		logio = os.Stderr
		h.DumpBucket(logio)
		h.DumpEntry(logio)
		logio = old
	}
}

func (h *Map) _update(item MapItem, v interface{}) bool {
	return item.SetValue(v)
}

// updateStable stores v into item, the entry of the key of k and conflict,
// as _update does. When an expand of an item pool ran meanwhile, it may have
// copied item before the store, so the store goes again into the entry that
// the key has after the expand.
func (h *Map) updateStable(item MapItem, k, conflict uint64, v interface{}) bool {
	seq := seqBeforeWrite()
	ok := h._update(item, v)
	if writeOverlappedExpand(seq) {
		if again, _, found := h._loadItem(k, conflict, nil); found && again.PtrListHead() != item.PtrListHead() {
			return h._update(again, v)
		}
	}
	return ok
}

func (h *Map) _validateallbucket() {

	for bucket := bucketFromListHead(h.headBucket.Next()); bucket != bucket.nextAsB(); bucket = bucket.nextAsB() {
		if bucket.itemPool().validateItems() != nil {
			bucket.itemPool().validateItems()
			// lget := lastgets
			// _ = lget
			cnt := madeBucket
			_ = cnt
		}
	}

}

// TestSet ... _set() for Test
func (h *Map) TestSet(k, conflict uint64, btable *bucket, item MapItem) bool {
	return h._set(k, conflict, btable.toBase(), item)
}

func (h *Map) _set(k, conflict uint64, btable *bucket, item MapItem) bool {
	ok, _ := h.setItem(k, conflict, btable, item, false)
	return ok
}

// setItem links item as _set does, and reports also whether it linked item
// and counted it in the length. fromUser tells an item that StoreItem got
// from its caller, which the map refuses while it is linked and not deleted:
// it lies in another map. An item from the pool of the map may be a slot
// reused while it was still linked; it is taken out and linked again.
func (h *Map) setItem(k, conflict uint64, btable *bucket, item MapItem, fromUser bool) (ok, linked bool) {

	if !h.isEmbededItemInBucket {
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().reverse, 0, bits.Reverse64(k)) {
			Log(LogDebug, "already set reverse")
		}
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().conflict, 0, conflict) {
			Log(LogDebug, "already set conflict")
		}
	}

	h.initBeforeSet()

	var addOpt HMethodOpt
	_ = addOpt
	//defer btable._validateItemsNear()

	if btable != nil {
		goto SKIP_FETCH_BUCKET
	}

	if h.modeForBucket < CombineSearch && h.modeForBucket > CombineSearch4 {
		btable = h.searchBucket(k)
	} else {
		btable, _ = h.searchBucket4update(k)

		for btable.reverse > item.PtrMapHead().reverse {
			if btable == btable.NextOnLevel() {
				break
			}
			btable = btable.NextOnLevel()
		}
		if btable.reverse > item.PtrMapHead().reverse {
			p := btable.PrevOnLevel()
			_ = p
			btable = h.searchBucket(k)
		}

	}
SKIP_FETCH_BUCKET:

	if btable != nil && btable.head() == nil {
		Log(LogWarn, "bucket.head not set")
	}
	if btable == nil || btable.head() == nil {
		btable = newBucket()
		//btable.head = h.head.Prev().Next()
	} else {
		addOpt = WithBucket(btable)
	}
	if !h.isEmbededItemInBucket && btable.head().Empty() {
		if IsInfo() {
			nbtable := btable.nextAsB()
			pbtable := btable.prevAsB()
			_ = nbtable
			_ = pbtable
		}
		btable.head()
	}

	entry, cnt := h.find(btable.head(), func(item HMapEntry) bool {
		mHead := item.PtrMapHead()
		return bits.Reverse64(k) <= mHead.reverse
	})
	_ = cnt

	var pEntry HMapEntry
	var tStart *elist_head.ListHead
	if entry != nil {
		pEntry = entry.Prev()
		erk := entry.PtrMapHead().reverse
		prk := pEntry.PtrMapHead().reverse
		rk := bits.Reverse64(k)
		_, _, _ = erk, prk, rk

		if entry.PtrMapHead().reverse < bits.Reverse64(k) {
			tStart = entry.PtrListHead()
		} else if pEntry.PtrMapHead().reverse < bits.Reverse64(k) {
			tStart = pEntry.PtrListHead()
		} else {
			Log(LogDebug, "hash key == reverse hash key")
		}
	}
	if tStart == nil {
		tStart = btable.head()
	}

	stepAt("set.beforeInit", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(tStart))
	if !item.PtrMapHead().claimLink() {
		// another store of item is linking it; this store is done when
		// that one is
		stepAt("set.waitLinked", unsafe.Pointer(item.PtrListHead()), nil)
		item.PtrMapHead().waitLinked()
		return true, false
	}
	var split *bucket
	defer func() {
		item.PtrMapHead().releaseLink()
		if split != nil {
			// split after the release: the split hands item over to a new
			// bucket, whose Set may copy item
			h.makeBucket2(split)
		}
	}()
	if !item.PtrListHead().IsSingle() {
		if fromUser && !item.PtrMapHead().IsDeleted() {
			// item is linked and not deleted, in another map: one item
			// cannot be linked twice
			return false, false
		}
		// a deleted item stays linked until it is taken out; take it out
		// before linking it again
		item.PtrListHead().MarkForDelete()
		for ok, _ := item.PtrListHead().IsSafety(); !ok; ok, _ = item.PtrListHead().IsSafety() {
			runtime.Gosched()
		}
	}
	atomic.AndUint32((*uint32)(&item.PtrMapHead().state), ^uint32(mapIsDeleted))
	item.PtrListHead().Init()
	if addOpt == nil {
		//btable._validateItemsNear()
		linked, split = h.add2(tStart, item)
		//btable._validateItemsNear()
	} else {
		//btable._validateItemsNear()
		linked, split = h.add2(tStart, item, addOpt)
		//btable._validateItemsNear()
	}
	if !linked {
		// the value went into an entry of the key linked meanwhile
		return true, false
	}
	atomic.AddInt64(&h.len, 1)
	if btable.level() > 0 {
		atomic.AddInt32(&btable._len, 1)
	}
	if !h.isEmbededItemInBucket && btable != nil && int(btable.len()) > h.maxPerBucket {
		h.makeBucket(item.PtrListHead(), int(btable.len())/2)
	}

	return true, true
}

func (h *Map) get(key interface{}) (interface{}, bool) {
	e, success := h._get(KeyToHash(key))
	if e == nil {
		return e, success
	}
	return e.Value(), success
}

var Failreverse uint64 = 0

func (h *Map) _get(k, conflict uint64) (MapItem, bool) {
	if EnableStats {
		h.mu.Lock()
		DebugStats[CntOfGet]++
		h.mu.Unlock()
	}
	e := h.searchKey(k, true)
	if e == nil {
		if atomic.LoadUint64(&Failreverse) == 0 {
			atomic.CompareAndSwapUint64(&Failreverse, 0, bits.Reverse64(k))
		}
		return nil, false
	}
	if stepEnabled {
		stepAt("get.found", unsafe.Pointer(e.PtrListHead()), nil)
	}
	if e = matchConflict(e, bits.Reverse64(k), conflict); e == nil {
		return nil, false
	}
	return e.(MapItem), true

}

// matchConflict returns the entry of reverse and conflict among the entries
// of reverse next to e, which a lookup found as one end of them. Keys with
// the same reversed hash differ only in conflict, and they lie next to each
// other in the order they were linked.
func matchConflict(e HMapEntry, reverse, conflict uint64) HMapEntry {
	if atomic.LoadUint64(&e.PtrMapHead().reverse) != reverse {
		return nil
	}
	matches := func(cur *elist_head.ListHead) (HMapEntry, bool) {
		if cur.DirectNext() == cur || cur.DirectPrev() == cur {
			return nil, false
		}
		c := e.HmapEntryFromListHead(cur)
		if atomic.LoadUint64(&c.PtrMapHead().reverse) != reverse {
			return nil, false
		}
		if !c.PtrMapHead().IsIgnored() && atomic.LoadUint64(&c.PtrMapHead().conflict) == conflict {
			return c, true
		}
		return nil, true
	}
	for cur := e.PtrListHead(); ; cur = cur.DirectNext() {
		c, same := matches(cur)
		if c != nil {
			return c
		}
		if !same {
			break
		}
	}
	for cur := e.PtrListHead().DirectPrev(); ; cur = cur.DirectPrev() {
		c, same := matches(cur)
		if c != nil {
			return c
		}
		if !same {
			break
		}
	}
	return nil
}

func (h *Map) getWithBucket(k, conflict uint64) (MapItem, *bucket, bool) {

	if EnableStats {
		h.mu.Lock()
		DebugStats[CntOfGet]++
		h.mu.Unlock()
	}
	var bucket *bucket
	var reverse uint64
	var e HMapEntry

	if h.isEmbededItemInBucket {
		reverse = bits.Reverse64(k)
		bucket = h.findBucket(reverse)
		e = h.bsearchBybucket(bucket, reverse, true)

	} else {
		bucket, reverse = h.searchBucket4update(k)
		if !h.isEmbededItemInBucket && bucket.head() == nil {
			bucket, reverse = h.searchBucket4update(k)
		}
		e = h.searchBybucket(bucket, reverse, true)
	}

	if e == nil {
		if atomic.LoadUint64(&Failreverse) == 0 {
			atomic.CompareAndSwapUint64(&Failreverse, 0, bits.Reverse64(k))
		}
		return nil, bucket, false
	}
	if e = matchConflict(e, bits.Reverse64(k), conflict); e == nil {
		return nil, bucket, false
	}

	return e.(MapItem), bucket, true

}

func (h *Map) notHaveBuckets() bool {
	return h.tailBucket.Next().Prev().Empty()
}

func levelMask(level int) (mask uint64) {
	mask = 0
	for i := 0; i < level; i++ {
		mask = (mask << 4) | 0xf
	}
	return
}

func (h *Map) searchBucket(k uint64) (result *bucket) {
	cnt := 0

	idx := bits.Reverse64(k) >> (4 * 15)
	for cur := &h.buckets[idx].ListHead; !cur.Empty(); cur = cur.DirectNext() {
		bcur := bucketFromListHead(cur)
		if bits.Reverse64(k) > bcur.reverse {
			return bcur
		}
		cnt++
	}
	return
}

// Get ... return the value for a key, if not found, ok is false
func (h *Map) Get(key interface{}) (value interface{}, ok bool) {
	item, ok := h._get(KeyToHash(key))
	if !ok {
		return nil, false
	}
	return item.Value(), ok
}

func (h *Map) GetByHash(hash, conflict uint64) (value interface{}, ok bool) {
	item, ok := h._get(hash, conflict)
	if !ok {
		return nil, false
	}
	return item.Value(), ok
}

// LoadItem ... return key/value item with embedded-linked-list. if not found, ok is false
//
// An item stored by Set lives in the map's item pool, and a later Set of a
// new key can move it to a new array: when the pool grows, and with
// UseEmbeddedPool also on an insert that is not at the end of the bucket's
// array. After such a Set, a read through the returned item can be a stale
// read and a write through it a lost update. With UseEmbeddedPool, the slot
// of a deleted item can also be reused for another key; if the returned item
// still points to that slot, it then reads and writes that key's entry. So
// use the returned item only when it is read, and change entries through the
// map's methods (Set, Delete, Purge). If the caller serializes all writes,
// the item stays valid until the next write. An item stored by StoreItem is
// kept alive by the caller and never moves.
func (h *Map) LoadItem(key interface{}) (item MapItem, success bool) {
	item, _, success = h._loadItem(0, 0, key)
	return
}

// LoadItemByHash ... LoadItem by the hash pair of KeyToHash.
// The returned item is valid only when it is read, as described in LoadItem.
func (h *Map) LoadItemByHash(k uint64, conflict uint64) (item MapItem, success bool) {

	item, success = h._get(k, conflict)
	return
}

func (h *Map) loadItem(k uint64, conflict uint64, key interface{}) (item MapItem, bucket *bucket, lock *trylock.Mutex, found bool) {

	for {
		item, bucket, found = h._loadItem(0, 0, key)
		if !found {
			break
		}
		if stepEnabled {
			stepAt("loadItem.found", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(bucket))
		}
		if mu, ok := h.lockFoundItem(key, item, bucket); ok {
			lock = mu
			break
		}
		runtime.Gosched()
	}
	return
}

// lockFoundItem locks the muPool that guards the pool of bucket, the one of
// the base bucket, which a Set of a new key locks too. It keeps the lock only
// when the lookup of key still finds item in the same pool, since the item
// may be purged and its slot reused before the lock.
func (h *Map) lockFoundItem(key interface{}, item MapItem, bucket *bucket) (*trylock.Mutex, bool) {
	mu := &bucket.toBase().muPool
	if !mu.TryLock() {
		return nil, false
	}
	again, b, found := h._loadItem(0, 0, key)
	if found && again.PtrListHead() == item.PtrListHead() && b.toBase() == bucket.toBase() {
		return mu, true
	}
	mu.Unlock()
	return nil, false
}

func (h *Map) _loadItem(k uint64, conflict uint64, key interface{}) (MapItem, *bucket, bool) {
	//return h._get(k, conflict)
	if key == nil {
		return h.getWithBucket(k, conflict)
	}

	return h.getWithBucket(KeyToHash(key))
}

var madeBucket int32 = 0

// Set ... set the value for a key
//
// When Set adds a new key, the item that holds it lives in the map's item
// pool, whose array keeps it reachable for the GC. A map without
// UseEmbeddedPool creates the pool on the first such Set if it has none. For
// a key already present, Set stores only the value into the existing item.
func (h *Map) Set(key, value interface{}) bool {
	atomic.StoreInt32(&madeBucket, 0)

	var item MapItem
	var bucket *bucket
	var found bool
	var seq uint64
	var fn func()
	release := func() {
		if fn != nil {
			fn()
			fn = nil
		}
	}

	for {
		item, bucket, found = h._loadItem(0, 0, key)
		if !found {
			break
		}
		if !h.isEmbededItemInBucket {
			k, conflict := KeyToHash(key)
			return h.updateStable(item, k, conflict, value)
		}
		if stepEnabled {
			stepAt("set.updateFound", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(bucket))
		}
		if mu, ok := h.lockFoundItem(key, item, bucket); ok {
			defer mu.Unlock()
			if stepEnabled {
				stepAt("set.updateLocked", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(bucket))
			}
			return h._update(item, value)
		}
		runtime.Gosched()
	}

	var s *SampleItem
	useDump := false

	if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&h.pooler))) == nil && !h.isEmbededItemInBucket {
		UsePool(true)(h)
	}
	if !h.isEmbededItemInBucket && bucket.head().Empty() {
		bucket.head()
		h._loadItem(0, 0, key)
		Log(LogDebug, "empty is invalid")
	}

	if h.isEmbededItemInBucket {
		k, conflict := KeyToHash(key)
		var nPool *samepleItemPool

		// Hold the bucket lock from the existence re-check to the link, so two
		// goroutines inserting the same key cannot both miss and both insert.
		for {
			mu := &bucket.toBase().muPool
			stepAt("set.newKeyLock", unsafe.Pointer(bucket), unsafe.Pointer(mu))
			mu.Lock()
			nb := bucket
			if item, nb, found = h._loadItem(0, 0, key); found {
				defer mu.Unlock()
				return h._update(item, value)
			}
			if nb.toBase() == bucket.toBase() {
				defer mu.Unlock()
				break
			}
			mu.Unlock()
			bucket = nb
		}

		//lastgets = nil
		oPool := bucket.itemPool()
		item, nPool, _ := oPool.getWithFn(bits.Reverse64(k), nil)

		s = item.(*SampleItem)
		if nPool != nil {
			bucket.setItemPool(nPool)
		}
		if stepEnabled {
			stepAt("set.slotTaken", unsafe.Pointer(s.PtrListHead()), unsafe.Pointer(bucket))
		}

		// s.PtrMapHead().reverse = bits.Reverse64(k)
		// s.PtrMapHead().conflict = conflict
		if !atomic.CompareAndSwapUint64(&s.PtrMapHead().reverse, 0, bits.Reverse64(k)) {
			Log(LogDebug, "already set reverse")
		}
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().conflict, 0, conflict) {
			Log(LogDebug, "already set conflict")
		}
	} else {
		k, _ := KeyToHash(key)
		seq = seqBeforeWrite()
		var wg sync.WaitGroup
		fn = nil
		wg.Add(1)
		pooler := (*Pool)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&h.pooler))))
		pooler.Get(bits.Reverse64(k), func(item MapItem, mu sync.Locker) {
			s = item.(*SampleItem)
			if mu != nil {
				fn = func() {
					mu.Unlock()
				}
			}
			wg.Done()
		})
		defer release()

		if s != nil && !s.IsSingle() {
			Log(LogWarn, "get not single entry?")
		}
		wg.Wait()
		if IsExtended {
			useDump = true
			IsExtended = false
		}
		if useDump {
			var b strings.Builder
			fmt.Fprintf(&b, "dump: bucket and entry\n")
			h.DumpBucket(&b)
			h.DumpEntry(&b)
			fmt.Fprintf(&b, "end: bucket and entry\n")
			fmt.Println(b.String())
			useDump = false
		}
	}

	s.K = key.(string)
	s.SetValue(value)

	if _, ok := h.ItemFn().(*SampleItem); !ok {
		ItemFn(func() MapItem {
			return EmptySampleHMapEntry
		})(h)
	}

	k, conflict := KeyToHash(s.K)
	if h.isEmbededItemInBucket {
		return h._set(k, conflict, bucket.toBase(), s)
	}
	if !s.IsSingle() {
		Log(LogWarn, "is not single")
	}
	ok, linked := h.setItem(k, conflict, bucket, s, false)
	// the pool counts s as linked only after this; an expand that waits
	// for it must not be waited for here
	release()
	if linked && writeOverlappedExpand(seq) {
		// an expand of an item pool ran during this Set: the link of s
		// may have gone into an array that no pool holds any more. s is
		// lost when a lookup misses it and nothing deleted it, which only
		// a delete that found s can do
		if stepEnabled {
			stepAt("set.expandOverlapped", unsafe.Pointer(s.PtrListHead()), nil)
		}
		if _, _, found := h._loadItem(k, conflict, nil); !found && !s.IsDeleted() {
			h.AddLen(-1)
			return h.Set(key, value)
		}
	}
	return ok
}

// StoreItem ... set key/value item with embedded-linked-list
//
// StoreItem links item itself into the map and does not copy it. The map
// holds only offsets to item, which the GC does not follow, so the caller
// must keep item reachable until it stops using the map, even after Delete
// or Purge, because a deleted item stays linked. Otherwise the map links
// freed memory (a dangling reference). The map never moves item. If the key
// is already present, only the value is stored into the existing item, and
// item is not linked. An item deleted by Delete or Purge can be stored again;
// an item linked in another map is not linked, and StoreItem returns false.
// Use StoreItem only on maps without UseEmbeddedPool:
// there item is linked but cannot be found.
func (h *Map) StoreItem(item MapItem) bool {
	k, conflict := item.KeyHash()

	oitem, bucket, found := h._loadItem(k, conflict, nil)
	if found {
		return h.updateStable(oitem, k, conflict, item.Value())
	}

	seq := seqBeforeWrite()
	ok, linked := h.setItem(k, conflict, bucket, item, true)
	if linked && writeOverlappedExpand(seq) {
		// an expand of an item pool ran during the link: item may be linked
		// only to an array that no pool holds any more. item is lost when a
		// lookup does not find it and nothing deleted it, which only a
		// delete that found item can do; then no live node leads to it
		if stepEnabled {
			stepAt("set.expandOverlapped", unsafe.Pointer(item.PtrListHead()), nil)
		}
		again, _, found := h._loadItem(k, conflict, nil)
		if (!found || again.PtrListHead() != item.PtrListHead()) && !item.PtrMapHead().IsDeleted() {
			item.PtrListHead().Init()
			h.AddLen(-1)
			return h.StoreItem(item)
		}
	}
	return ok
}

func (h *Map) eachEntry(start *elist_head.ListHead, fn func(*entryHMap)) {
	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead(cur)
		if e.key == nil {
			continue
		}
		fn(e)
	}
	return
}

func (h *Map) each(start *elist_head.ListHead, fn func(key, value interface{})) {

	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead(cur)
		fn(e.key, e.value)
	}
	return
}

// must renename to find
func (h *Map) find(start *elist_head.ListHead, cond func(HMapEntry) bool) (result HMapEntry, cnt int) {

	stepAt("find.begin", unsafe.Pointer(start), nil)
	cnt = 0
	var e MapItem
	if start.Empty() {
		return
	}
	for cur := start; cur != cur.Next(); cur = cur.Next() {
		e = entryHMapFromListHead(cur)

		if cond(e) {
			result = e
			return
		}
		cnt++
	}
	return nil, cnt

}

func (h *Map) makeBucket(ocur *elist_head.ListHead, back int) (err error) {

	stepAt("makeBucket.begin", unsafe.Pointer(ocur), nil)
	enableDumpBucket := false

	cur := ocur
	cur = cur.Prev()

	e := entryHMapFromListHead(cur)
	cBucket := h.searchBucket(bits.Reverse64(e.reverse))
	if cBucket == nil || cBucket.reverse > e.reverse {
		return ErrBucketNotFound
	}
	nextBucket := cBucket
	for ; nextBucket.prevAsB() != nextBucket; nextBucket = nextBucket.prevAsB() {
		stepAt("makeBucket.pairWalk", unsafe.Pointer(nextBucket), unsafe.Pointer(cBucket))

		if nextBucket.reverse > e.reverse {
			break
		}
		if nextBucket.reverse <= e.reverse && nextBucket.reverse > cBucket.reverse {
			cBucket = nextBucket
		}
	}
	if nextBucket.reverse < e.reverse || cBucket.reverse > e.reverse {
		return ErrBucketNotFound
	}

	newReverse := cBucket.reverse / 2
	if nextBucket.reverse == ^uint64(0) && cBucket.reverse == 0 {
		newReverse = bits.Reverse64(0x1)
	} else if cBucket == nextBucket { //FIXME:  invalid pattern?
		newReverse = cBucket.reverse / 2
	} else if nextBucket.reverse == ^uint64(0) {
		newReverse += ^uint64(0) / 2
		newReverse += 1
	} else {
		newReverse = halfUint64(cBucket.reverse, nextBucket.reverse)
	}

	stepAt("makeBucket.pairFound", unsafe.Pointer(cBucket), unsafe.Pointer(nextBucket))
	b, onOk := h.bucketFromPool(newReverse, useOnOk(true))
	stepAt("makeBucket.claimed", unsafe.Pointer(b), unsafe.Pointer(ocur))
	if b == nil {
		return ErrBucketAlreadyExit
	}
	if onOk == nil {
		Log(LogWarn, "no okFn")
	}
	defer func() {
		if onOk != nil {
			onOk()
		} else {
			Log(LogWarn, "no okFn")
		}
		if atomic.LoadUint32(&b.state) != bucketStateActive {
			Log(LogWarn, "not active?")
		}
	}()
	if !b.headNoWaitEmpty().Empty() {
		Log(LogDebug, "not empty")
		return ErrBucketAlreadyExit
	} else {
		Log(LogDebug, "empty")
	}

	if b == nil {
		if IsDebug() {
			h.searchBucket(bits.Reverse64(e.reverse))
		}

		return ErrBucketAllocatedFail
	}
	//b.reverse, b.level, b.len = newReverse, level, 0
	if b.reverse != newReverse {
		b.reverse, b._len = newReverse, 0
	}

	if b.reverse == 0 && b.level() > 1 {
		err = NewError(EBucketInvalid, "bucket.reverse = 0. but level 1= 1", nil)
		Log(LogWarn, "%s", err.Error())
		return
	}

	stepAt("makeBucket.beforeInit", unsafe.Pointer(b), nil)
	b.Init()
	b.LevelHead.Init()

	for cur := cBucket.head().Prev().Next(); !cur.Empty(); cur = cur.Next() {
		b._len++
		e := entryHMapFromListHead(cur)
		if e.reverse > b.reverse {
			break
		}
	}

	atomic.AddInt32(&cBucket._len, -b._len)
	err = h.addBucket(b)
	if err != nil {
		Log(LogWarn, "fail addBucket() e=%+v\n", err)
	}
	stepAt("makeBucket.added", unsafe.Pointer(b), nil)
	if !h.isEmbededItemInBucket && b.head().Empty() {
		panic("bucket head empty")
	}
	if l := b.level(); l < 0 {
		b.setLevel(-l)
	}

	if b.LevelHead.DirectNext() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	h.insertOnLevel(b, b.level(), "makeBucket.levelFound", unsafe.Pointer(b))
	if b.LevelHead.Next() == &b.LevelHead {
		Log(LogWarn, "bucket.LevelHead is pointed to self")
	}

	if enableDumpBucket {
		w := logio

		if w == io.Discard {
			w = os.Stderr
		}
		h.DumpBucket(w)
	}

	return

}

type hmapMethod struct {
	bucket *bucket
}

type HMethodOpt func(*hmapMethod)

func WithBucket(b *bucket) func(*hmapMethod) {

	return func(conf *hmapMethod) {
		conf.bucket = b
	}
}

func (h *Map) add2(start *elist_head.ListHead, e HMapEntry, opts ...HMethodOpt) (linked bool, split *bucket) {
	var opt *hmapMethod
	if len(opts) > 0 {
		opt = &hmapMethod{}
		for _, fn := range opts {
			fn(opt)
		}
	}

	cnt := 0

	defer func() {
		if !EnableStats || e.PtrMapHead().IsIgnored() {
			return
		}

		if h.SearchKey(bits.Reverse64(e.PtrMapHead().reverse), ignoreBucketEntry(false)) == nil {
			o := sharedSearchOpt(nil)
			o.Lock()
			o.e = ErrItemInvalidAdd
			o.Unlock()
			sharedSearchOpt(o)
		}

	}()

RETRY:
	pos, _ := h.find(start, func(ehead HMapEntry) bool {
		cnt++
		if !e.PtrListHead().IsSingle() {
			Log(LogWarn, "add2: element for insertion  is not single ")
		}
		return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
	})
	if stepEnabled && pos != nil {
		stepAt("add2.found", unsafe.Pointer(e.PtrListHead()), unsafe.Pointer(pos.PtrListHead()))
	}
	if !e.PtrListHead().IsSingle() {

		// rev := e.PtrMapHead().reverse
		// idx := (rev >> (4 * 15) % cntOfPoolMgr)
		// p := samepleItemPoolFromListHead(h.pooler.itemPool[idx].Next())
		// _ = p
		if IsInfo() {
			pools := h.allpools()
			_ = pools
			e.PtrListHead().IsSingle()
		}
		Log(LogWarn, "add2: element for insertion  is not single ")
	}

	if pos != nil {
		if !e.PtrListHead().IsSingle() {
			Log(LogWarn, "add2: element for insertion  is not single ")
			err := e.PtrListHead().MarkForDelete()
			if err != nil {
				Log(LogError, "fail delete")
			}
			e.PtrListHead().Init()
		}

		if h.storeIntoSameKey(pos.PtrListHead(), e) {
			return false, nil
		}
		if err := insertInOrder(pos.PtrListHead(), e.PtrListHead()); err != nil {
			goto RETRY
		}
		if opt == nil || opt.bucket == nil {
			return true, nil
		}
		btable := opt.bucket
		if btable == nil || e.PtrMapHead().IsIgnored() || int(btable.len()) <= h.maxPerBucket {
			return true, nil
		}

		// FIXME: not run on !h.isEmbededItemInBucket
		if !h.isEmbededItemInBucket {
			//h.makeBucket(e.PtrListHead(), int(btable.len())/2)
		} else {
			// the caller splits btable when it is done with e: the split
			// hands e over to a new bucket, whose Set may copy e
			split = btable
		}

		return true, split
	}
	if opt != nil && opt.bucket != nil && opt.bucket.entry(h) != nil {
		// pos, _ = h.find(start, func(ehead HMapEntry) bool {
		// 	return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
		// }, ignoreBucketEntry(false))
		nextE := nextAsE(opt.bucket.entry(h))
		if nextE.PtrMapHead().reverse <= EmptyMapHead.fromListHead(opt.bucket.head()).reverse {
			Log(LogWarn, "map.add2() re-try get target")
			mHead := EmptyMapHead.fromListHead(opt.bucket.head().Next())
			_ = mHead
			nextE = opt.bucket.prevAsB().entry(h)
		}

		stepAt("add2.bucketInsert", unsafe.Pointer(e.PtrListHead()), unsafe.Pointer(nextE.PtrListHead()))
		if h.storeIntoSameKey(nextE.PtrListHead(), e) {
			return false, nil
		}
		if err := insertInOrder(nextE.PtrListHead(), e.PtrListHead()); err == nil {
			return true, nil
		}
		// the entry after the dummy of the bucket is not a place for e;
		// no entry comes after e, so e goes just before the last one
	}
	pos, _ = h.find(start, func(ehead HMapEntry) bool {
		cnt++
		return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
	})
	if pos != nil {
		goto RETRY
	}

	if stepEnabled {
		stepAt("add2.tailInsert", unsafe.Pointer(e.PtrListHead()), unsafe.Pointer(h.tail.Prev()))
	}
	if h.storeIntoSameKey(h.tail.Prev(), e) {
		return false, nil
	}
	if err := insertInOrder(h.tail.Prev(), e.PtrListHead()); err != nil {
		goto RETRY
	}
	return true, nil
}

// storeIntoSameKey stores the value of e into the entry just before right
// when that entry is a live entry of the key of e, which another store
// linked after the lookup of e missed the key. It reports whether it did.
func (h *Map) storeIntoSameKey(right *elist_head.ListHead, e HMapEntry) bool {
	left := right.DirectPrev()
	if left == right || !sameKeyLinked(mapheadFromLListHead(left), e.PtrMapHead()) {
		return false
	}
	if item, ok := e.(MapItem); ok {
		if old, ok := e.HmapEntryFromListHead(left).(MapItem); ok {
			h._update(old, item.Value())
		}
	}
	return true
}

func (h *Map) BackBucket() (bCur *bucket) {

	for cur := h.headBucket.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		bCur = bucketFromListHead(cur)
	}
	return
}

func (h *Map) toFrontBucket(bucket *bucket) (result *bucket) {

	for cur := bucket.PtrListHead(); !cur.Empty(); cur = cur.DirectPrev() {
		result = bucketFromListHead(cur)
	}
	return
}

func (h *Map) toBackBucket(bucket *bucket) (result *bucket) {

	for cur := bucket.PtrListHead(); !cur.Empty(); cur = cur.DirectNext() {
		result = bucketFromListHead(cur)
	}
	return
}

func (h *Map) DumpBucket(w io.Writer) {
	var b strings.Builder

	for cur := h.headBucket.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		btable := bucketFromListHead(cur)
		fmt.Fprintf(&b, "  bucket{reverse: 0x%16x, len: %d, start: %p, level{%d, cur: %p, prev: %p next: %p} down: %d}\n",
			btable.reverse, btable.len(), btable.head, btable.level(), &btable.LevelHead, btable.LevelHead.DirectPrev(), btable.LevelHead.DirectNext(), btable.ptrDownLevels().Len())
	}
	if w == nil {
		os.Stdout.WriteString(b.String())
		return
	}
	w.Write([]byte(b.String()))

}

func (h *Map) DumpBucketPerLevel(w io.Writer) {
	var b strings.Builder

	for i := range h.levels {
		cBucket := h.levelBucket(int32(i) + 1)
		if cBucket == nil {
			continue
		}
		if h.isEmptyBylevel(int32(i) + 1) {
			continue
		}
		fmt.Fprintf(&b, "bucket level=%d\n", i+1)
		for cur := cBucket.LevelHead.DirectPrev().DirectNext(); !cur.Empty(); {
			cBucket = bucketFromLevelHead(cur)
			cur = cBucket.LevelHead.DirectNext()
			fmt.Fprintf(&b, "  bucket{reverse: 0x%16x, len: %d, start: %p, level{%d, cur: %p, prev: %p next: %p} down: %d}\n",
				cBucket.reverse, cBucket.len(), cBucket.head, cBucket.level(), &cBucket.LevelHead, cBucket.LevelHead.DirectPrev(), cBucket.LevelHead.DirectNext(), cBucket.ptrDownLevels().Len())

		}
	}
	if w == nil {
		os.Stdout.WriteString(b.String())
		return
	}
	w.Write([]byte(b.String()))

}

func (h *Map) DumpEntry(w io.Writer) {
	var b strings.Builder

	for cur := h.head.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		//var e HMapEntry
		//e = e.HmapEntryFromListHead(cur)
		mhead := EmptyMapHead.FromListHead(cur)
		e := fromMapHead(mhead)

		var ekey interface{}
		ekey = e.Key()
		fmt.Fprintf(&b, "  entryHMap{key: %+10v, k: 0x%16x, reverse: 0x%16x), conflict: 0x%x, cur: %p, prev: %p, next: %p}\n",
			ekey, bits.Reverse64(mhead.reverse), mhead.reverse, mhead.conflict, mhead.PtrListHead(), mhead.PtrListHead().DirectPrev(), mhead.PtrListHead().DirectNext())
	}

	if w == nil {
		os.Stdout.WriteString(b.String())
		return
	}
	w.Write([]byte(b.String()))

}

func toMask(level int) (mask uint64) {

	for i := 0; i < level; i++ {
		if mask == 0 {
			mask = 0xf
			continue
		}
		mask = (mask << 4) | 0xf
	}
	return
}

func toMaskR(level int) (mask uint64) {

	for i := 0; i < level+1; i++ {
		if mask == 0 {
			mask = 0xf << ((16 - i) * 4)
			continue
		}
		mask |= (0xf << ((16 - i) * 4))
	}
	return
}

func reverse2Index(level int, r uint64) (idx int) {

	return int((r & toMaskR(level)) >> ((16 - level) * 4))

}

func (h *Map) _InsertBefore(tBtable *list_head.ListHead, nBtable *bucket) {

	stepAt("insertBucket.begin", unsafe.Pointer(nBtable), nil)
	empty := &nBtable.dummy
	empty.key, empty.value = nil, nil
	empty.reverse, empty.conflict = nBtable.reverse, 0
	empty.PtrMapHead().state |= mapIsDummy
	empty.Init()
	var thead *elist_head.ListHead
	if tBtable.Empty() {
		thead = h.head.Prev().Next()
	} else {
		tBucket := bucketFromListHead(tBtable)
		thead = tBucket.head().Prev().Next()
	}
	h.add2(thead, empty)
	stepAt("insertBucket.dummyLinked", unsafe.Pointer(nBtable), nil)
	if empty.ListHead.DirectPrev() == &empty.ListHead && empty.ListHead.DirectNext() == &empty.ListHead {
		Log(LogWarn, "fail register dummy of bucket")
	}

	// add bucket
	h.linkBucket(tBtable, nBtable)

	if IsDebug() {
		h.validateBucket((nBtable))
	}

}

func (h *Map) addBucket(nBtable *bucket) error {

	pos := h.bucketInsertPos(nBtable.reverse)
	if !pos.Empty() && bucketFromListHead(pos).reverse == nBtable.reverse {
		return ErrBucketAlreadyExit
	}
	h._InsertBefore(pos, nBtable)
	return nil
}

// bucketInsertPos returns the node of the list of buckets, which is in
// descending order of reverse, that a bucket of reverse goes before: the
// first bucket whose reverse is not larger, or the end of the list.
func (h *Map) bucketInsertPos(reverse uint64) *list_head.ListHead {
	pos := h.headBucket.Prev().Next()
	for !pos.Empty() && bucketFromListHead(pos).reverse > reverse {
		pos = pos.Next()
	}
	return pos
}

// linkBucket links nBtable into the list of buckets before pos. It links it
// only while the bucket before pos is larger, and finds the place again when
// another bucket was linked there meanwhile; it leaves the list as it is when
// a bucket of the same reverse is linked by then.
func (h *Map) linkBucket(pos *list_head.ListHead, nBtable *bucket) {
	for retry := 0; ; retry++ {
		if retry > 0 {
			runtime.Gosched()
			pos = h.bucketInsertPos(nBtable.reverse)
		}
		if !pos.Empty() && bucketFromListHead(pos).reverse == nBtable.reverse {
			return
		}
		err := pos.TryInsertBefore(&nBtable.ListHead, func(prev *list_head.ListHead) bool {
			return prev.Empty() || bucketFromListHead(prev).reverse > nBtable.reverse
		})
		if err == nil {
			return
		}
	}
}

// insertOnLevel links b into the list of level, which is in descending order
// of reverse: just before the first bucket with a smaller reverse, or at the
// end. It links b only while the bucket before that place is larger, and
// finds the place again when another bucket was linked there meanwhile.
// point names the step point just before the link, and at is its first
// argument.
func (h *Map) insertOnLevel(b *bucket, level int32, point string, at unsafe.Pointer) {
	for retry := 0; ; retry++ {
		if retry > 0 {
			runtime.Gosched()
		}
		pos := h.levelBucket(level).LevelHead.Next()
		for !pos.Empty() && bucketFromLevelHead(pos).reverse > b.reverse {
			pos = pos.Next()
		}
		if !pos.Empty() && bucketFromLevelHead(pos).reverse == b.reverse {
			// a bucket of the same reverse is on the level already
			return
		}
		if stepEnabled {
			var found *list_head.ListHead
			if !pos.Empty() {
				found = pos
			}
			stepAt(point, at, unsafe.Pointer(found))
		}
		err := pos.TryInsertBefore(&b.LevelHead, func(prev *list_head.ListHead) bool {
			return prev.Empty() || bucketFromLevelHead(prev).reverse > b.reverse
		})
		if err == nil {
			return
		}
	}
}

func (h *Map) initLevels() {

	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.levels {
		b := newBucket()
		b.setLevel(int32(i) + 1)
		b.LevelHead.InitAsEmpty()
		h.levels[i].Store(b)
	}
}

func (h *Map) setLevel(level int32, b *bucket) bool {

	return false
}

func (h *Map) levelBucket(level int32) (b *bucket) {
	ov := h.levels[level-1]
	b = ov.Load().(*bucket)

	return b
}

func (h *Map) isEmptyBylevel(level int32) bool {
	if int32(len(h.levels)) < level {
		return true
	}
	b := h.levelBucket(level)

	if b.Empty() {
		return true
	}

	prev := b.LevelHead.DirectPrev()
	next := b.LevelHead.DirectNext()

	if prev == prev.DirectPrev() && next == next.DirectNext() {
		return true
	}
	return false
}

const (
	CntSearchBucket  statKey = 1
	CntLevelBucket   statKey = 2
	CntSearchEntry   statKey = 3
	CntReverseSearch statKey = 4
	CntOfGet         statKey = 5
)

func nextNoCheck(e HMapEntry) HMapEntry {
	return e.Next()
}

func prevNoCheck(e HMapEntry) HMapEntry {
	return e.Prev()
}

func nextAsE(e HMapEntry) HMapEntry {
	start := e.PtrListHead()
	if !start.DirectNext().Empty() {
		start = start.DirectNext()
	}
	if !start.Empty() {
		return e.HmapEntryFromListHead(start)
	}
	return nil
}

func prevAsE(e HMapEntry) HMapEntry {
	start := e.PtrListHead()
	if !start.DirectPrev().Empty() {
		start = start.DirectPrev()
	}
	if !start.Empty() {
		return e.HmapEntryFromListHead(start)
	}
	return nil
}

var _sharedSearchOpt atomic.Value

func init() {
	// lista reads this for every list, so it is set once, before any
	// goroutine uses a map
	list_head.MODE_CONCURRENT = true

	o := &searchOpt{}
	o._ignoreBucketEntry.Store(true)

	_sharedSearchOpt.Store(o)
}

type searchOpt struct {
	h                  *Map
	e                  error
	_ignoreBucketEntry atomic.Value
	sync.Mutex
}

func sharedSearchOpt(setter *searchOpt) *searchOpt {

	if setter != nil {
		_sharedSearchOpt.Store(setter)
		return setter
	}
	return _sharedSearchOpt.Load().(*searchOpt)
}

func (o *searchOpt) ignoreBucketEntry() bool {
	return o._ignoreBucketEntry.Load().(bool)
}

type searchArg func(*searchOpt) searchArg

func ignoreBucketEntry(t bool) searchArg {

	return func(opt *searchOpt) searchArg {
		prev := opt._ignoreBucketEntry.Load().(bool)
		opt._ignoreBucketEntry.Store(t)
		return ignoreBucketEntry(prev)
	}
}

func (o *searchOpt) Options(opts ...searchArg) (previous searchArg) {

	o.Lock()
	defer o.Unlock()
	for _, fn := range opts {
		previous = fn(o)
	}
	return previous
}

// SearchKey ... search the entry of the hash k of KeyToHash.
// The returned entry is valid only when it is read, as described in LoadItem.
func (h *Map) SearchKey(k uint64, opts ...searchArg) HMapEntry {

	conf := sharedSearchOpt(nil)
	previous := conf.Options(opts...)
	defer func() {
		if previous != nil {
			conf.Options(previous)
			sharedSearchOpt(conf)
		}
	}()
	return h.searchKey(k, conf.ignoreBucketEntry())

}

func (h *Map) searchKey(k uint64, ignoreBucketEnry bool) HMapEntry {
	if h.isEmbededItemInBucket {
		return h.searchKeyFromEmbeddedPool(k, ignoreBucketEnry)
	}
	return h.searchBybucket(h.searchBucket4Key4(k, ignoreBucketEnry))
}

func (h *Map) topLevelBucket(reverse uint64) *bucket {
	idx := (reverse >> (4 * 15))

	return &h.buckets[idx]
}

func (h *Map) searchBucket4update(k uint64) (b *bucket, r uint64) {

	reverseNoMask := bits.Reverse64(k)

	cBuf := h.findBucket(reverseNoMask)
	if cBuf._itemPool == nil {
		p, n := cBuf.prevAsB(), cBuf.nextAsB()
		_, _ = p, n
		cBuf = h.findBucket(reverseNoMask)
		return cBuf, reverseNoMask
	}
	if cBuf.prevAsB() == cBuf || cBuf.prevAsB().reverse > reverseNoMask {
		return cBuf, reverseNoMask
	}
	for cur := cBuf; cur != cur.prevAsB(); cur = cur.prevAsB() {
		cRev := cur.prevAsB().reverse
		_ = cRev
		if cur.prevAsB().reverse > reverseNoMask {
			return cur, reverseNoMask
		}
	}
	return cBuf, reverseNoMask

}

func (h *Map) searchBucket4Key4(k uint64, ignoreBucketEnry bool) (b *bucket, reverse uint64, ignore bool) {
	b, reverse = h.searchBucket4update(k)
	if h.modeForBucket != CombineSearch {
		if p := b.prevAsB(); nearUint64(p.reverse, b.reverse, reverse) != b.reverse {
			b = p
		}
	}
	ignore = ignoreBucketEnry
	return
}

func nearBucketFromCache(levels [16]*bucket, lbNext *bucket, reverseNoMask uint64) (result *bucket) {
	noNil := true
	result = lbNext
	for i, b := range levels {
		if b == nil {
			noNil = false
			break
		}
		if int32(i)+1 == result.level() {
			continue
		}
		if nearUint64(b.reverse, result.reverse, reverseNoMask) == b.reverse {
			result = b
		}
	}
	if noNil {
		noNil = false
	}
	return
}

func (h *Map) searchBybucket(lbCur *bucket, reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry {
	if h.isEmbededItemInBucket {
		Log(LogWarn, "should use bsearchBybucket() not searchBybucket() in embedded bucket")
		return h.bsearchBybucket(lbCur, reverseNoMask, ignoreBucketEnry)
	}

	return h._searchBybucket(lbCur, reverseNoMask, ignoreBucketEnry)
}

func (h *Map) _searchBybucket(lbCur *bucket, reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry {
	if lbCur == nil {
		return nil
	}

	lbNext := lbCur

	if h.modeForBucket == CombineSearch2 && lbCur.reverse > reverseNoMask {
		lbNext = lbCur.NextOnLevel()
	}

	if lbNext.reverse < reverseNoMask {
		result := lbNext.entry(h)

		// FIXME: why fail to get
		var resultHead *MapHead
		if result == nil {
			result = lbNext.entry(h)
		}
		if result != nil {
			resultHead = result.PtrMapHead()
		}
		pCur := resultHead
		if IsDebug() {
			lbNext.itemPool().validateItems()
		}
		cnt := 0
		for cur := resultHead; cur != nil; cur = cur.NextWithNil() {
			cnt++
			curReverse := cur.reverse
			if cur.reverse == ^uint64(0) {
				Log(LogDebug, "last dummy item")
			}
			if pCur.reverse > cur.reverse {
				fmt.Println("invalid item order ")
			}

			pCur = cur
			if EnableStats && ignoreBucketEnry {
				h.mu.Lock()
				DebugStats[CntReverseSearch]++
				h.mu.Unlock()
			}
			if ignoreBucketEnry && cur.IsIgnored() {
				continue
			}

			if curReverse < reverseNoMask {
				continue
			}
			if curReverse == reverseNoMask {
				return result.HmapEntryFromListHead(cur.PtrListHead())
			}
			return nil
		}
		return nil
	}
	result := lbNext.entry(h)
	for cur := result.PtrMapHead(); cur != nil; cur = cur.PrevtWithNil() {
		if EnableStats && ignoreBucketEnry {
			h.mu.Lock()
			DebugStats[CntSearchEntry]++
			h.mu.Unlock()
		}
		curReverse := cur.reverse
		if ignoreBucketEnry && cur.IsIgnored() {
			continue
		}
		if curReverse > reverseNoMask {
			continue
		}
		if curReverse == reverseNoMask {
			return result.HmapEntryFromListHead(cur.PtrListHead())
		}

		return nil

	}
	return nil

}

// Delete ... set nil to the key of MapItem. cannot Get entry
func (h *Map) Delete(key interface{}) bool {

	for {
		item, ok := h.LoadItem(key)
		if !ok {
			return false
		}
		if stepEnabled {
			stepAt("delete.found", unsafe.Pointer(item.PtrListHead()), nil)
		}
		// an expand of the item pool of item must copy it after the mark
		// of the delete; hold the pool while marking
		var sp *samepleItemPool
		pooler := (*Pool)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&h.pooler))))
		if pooler != nil && !h.isEmbededItemInBucket {
			var expanding bool
			sp, expanding = pooler.holdItem(item.PtrMapHead().reverse, item)
			if expanding {
				runtime.Gosched()
				continue
			}
			if sp == nil {
				// no pool holds item now: an expand moved it, or it is
				// not from a pool
				if again, ok := h.LoadItem(key); ok && again.PtrListHead() != item.PtrListHead() {
					runtime.Gosched()
					continue
				}
			}
		}
		claimed := item.PtrMapHead().claimDelete()
		if claimed {
			item.Delete()
		}
		if sp != nil {
			sp.linking.Add(-1)
		}
		if !claimed {
			// another delete of the key got there first
			return false
		}
		h.AddLen(-1)
		return true
	}

}

// Purge ... key/value entry from map.
func (h *Map) Purge(key interface{}) bool {
	if h.isEmbededItemInBucket {
		return h.purgeInEmbedded(key)
	}
	return h.Delete(key)
}

func (h *Map) purgeInEmbedded(key interface{}) bool {
	var item MapItem
	var bucket *bucket
	var found bool
	var lock *trylock.Mutex
	_, _, _, _ = item, bucket, lock, found

	item, bucket, lock, found = h.loadItem(0, 0, key)
	if lock != nil {
		defer lock.Unlock()
	}
	if !found {
		return false
	}

	// remove item
	// 1. item status to remove
	// 2. purge from linked-list

	pool := bucket.itemPool()
	if pool == nil {
		return false
	}
	item.Delete()
	h.AddLen(-1)

	item.PtrListHead().MarkForDelete()
	if stepEnabled {
		stepAt("purge.beforeInit", unsafe.Pointer(item.PtrListHead()), nil)
	}
	item.PtrListHead().Init()

	pItems := pool.ptrItems()
	len := pItems.Len()
	if item.PtrListHead() == &pItems._at(len-1, true, false).ListHead &&
		atomic_util.CompareAndSwapInt(&pItems.len, len, len-1) {
		if stepEnabled {
			stepAt("purge.lenLowered", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(pool))
		}
		pool.shrinkLen()
		return true
	}

	// The slot stays in the array, unlinked, with mapIsDeleted set; that flag
	// is what getWithFn's foundFree path reuses. No free list: its offsets
	// would go stale when expand/insertToPool rebuild the array.
	return true
}

// RangeItem ... calls f sequentially for each key and value present in the map.
// called ordre is reverse key order
//
// The item passed to f is valid only when it is read, as described in
// LoadItem.
func (h *Map) RangeItem(f func(MapItem) bool) {

	// walk the links as they are, without a traversal mode shared with other
	// goroutines: a node being deleted still leads to the next one, and it
	// is skipped as deleted
	for cur := h.head.DirectPrev().DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
		mhead := EmptyMapHead.FromListHead(cur)
		if mhead.IsIgnored() {
			continue
		}
		e := h.ItemFn().HmapEntryFromListHead(mhead.PtrListHead()).(MapItem)
		if !f(e) {
			break
		}
	}
}

// Range ... calls f sequentially for each key and value present in the map.
// order is reverse key order
func (h *Map) Range(f func(key, value interface{}) bool) {

	h.RangeItem(func(item MapItem) bool {
		return f(item.Key(), item.Value())
	})
}

// First ... return the first entry of the list: the dummy entry of a bucket
// that starts the list, which is not in the item pool. Items reached from it
// by Next are valid only when read, as described in LoadItem.
func (h *Map) First() HMapEntry {
	cur := h.head.DirectPrev().DirectNext()
	return h.ItemFn().HmapEntryFromListHead(cur)
}

// Last ... return the last entry of the list: the dummy entry of a bucket
// that ends the list, which is not in the item pool. Items reached from it
// by Prev are valid only when read, as described in LoadItem.
func (h *Map) Last() HMapEntry {
	cur := h.tail.DirectNext().DirectPrev()
	return h.ItemFn().HmapEntryFromListHead(cur)
}

func (h *Map) allpools() (pools []*samepleItemPool) {
	if h.pooler == nil {
		return
	}

	for i := range h.pooler.itemPool {
		pools = append(pools, samepleItemPoolFromListHead(h.pooler.itemPool[i].Next()))
	}
	return
}

func (h *Map) FindBucket(reverse uint64) (b *bucket) {
	return h._findBucket(reverse, false, false)
}

func (h *Map) findBucket(reverse uint64) (b *bucket) {

	return h._findBucket(reverse, false, false)
}

func (h *Map) _findBucket(reverse uint64, ignoreNoPool bool, ignoreNoInitDummy bool) (b *bucket) {

	for l := 1; l < 16; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			// MENTION: if result pool is require, enable
			// if !h.isEmbededItemInBucket {
			// 	results = append(results, &h.buckets[idx])
			// }
			b = &h.buckets[idx]
			continue
		}
		var bucketDowns *bucketSlice
		bucketDowns = b.ptrDownLevels()
		if bucketDowns == nil || atomic_util.LoadInt(&bucketDowns.cap) == 0 {
			break
		}

		for {
			if atomic_util.LoadInt(&bucketDowns.len) > 0 {
				break
			}
		}

		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		if atomic_util.LoadInt(&bucketDowns.len) <= idx ||
			bucketDowns.at(idx).level() <= 0 {

			nidx := idx
			if downlen := atomic_util.LoadInt(&bucketDowns.len); nidx > downlen-1 {
				nidx = downlen - 1
			}
			for i := nidx; i > -1; i-- {
				if bucketDowns.at(i).level() <= 0 {
					continue
				}
				// FIXME: should not lookup direct
				if ignoreNoPool && bucketDowns.at(i)._itemPool == nil {
					continue
				}
				if ignoreNoInitDummy && atomic.LoadUint32(&bucketDowns.at(i).state) != bucketStateActive {
					continue
				}

				b = bucketDowns.at(i).largestDown(ignoreNoPool, ignoreNoInitDummy)
				return b

			}
			break
		}
		// FIXME: should not lookup direct
		if ignoreNoPool && bucketDowns.at(idx)._itemPool == nil {
			break
		}
		if ignoreNoInitDummy && atomic.LoadUint32(&bucketDowns.at(idx).state) != bucketStateActive {
			break
		}
		// MENTION: if result pool is require, enable
		// if !h.isEmbededItemInBucket {
		// 	results = append(results, bucketDowns.at(idx))
		// }

		b = bucketDowns.at(idx)
	}
	if b.level() == 0 {
		return nil
	}
	return
}

const recoverBucketWithOutInit bool = false

func (h *Map) bucketFromPool(reverse uint64, opts ...cOptFn) (b *bucket, onOk func()) {
	// h.mu.Lock()
	// defer h.mu.Unlock()

	opt := &commonOpt{}
	prevs := opt.Option(opts...)
	defer opt.Option(prevs...)

	level := int32(0)
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	bucketNotInits := []*bucket{}

	for l := int32(1); l <= level; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
	RETRY_INITIALIZE:
		if atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, 0, 1) {

			downLevels := make([]bucket, 1, 16)

			downLevels[0].setLevel(b.childLevel())
			downLevels[0].reverse = b.reverse
			downLevels[0].Init()
			downLevels[0].LevelHead.Init()
			downLevels[0]._parent = b
			downLevels[0].setItemPoolFn = func(p *samepleItemPool) {
				b.setItemPool(p)
			}

			h.insertOnLevel(&downLevels[0], l, "bucketFromPool.levelFound", unsafe.Pointer(b))
			downLevels[0].state = bucketStateInit
			// if idx != 0 && !h.isEmbededItemInBucket {
			// 	h.add2(b.head(), &b.downLevels[0].dummy)
			// }
			downs := b.ptrDownLevels()
			atomic.StorePointer(&downs.data, unsafe.Pointer(unsafe.SliceData(downLevels)))
			atomic_util.StoreInt(&downs.cap, cap(downLevels))
			atomic_util.StoreInt(&downs.len, len(downLevels))
			stepAt("bucketFromPool.lenStored", unsafe.Pointer(b), nil)
		} else if b.ptrDownLevels().Len() == 0 {
			if !recoverBucketWithOutInit {
				goto RETRY_INITIALIZE
			}
			if b.cntOfActiveLevels == 1 && cap(b.downLevels) > 1 {
				b.downLevels = b.downLevels[:b.cntOfActiveLevels]
			}
			goto RETRY_INITIALIZE
		}
	RETRY_SETUP:
		if atomic.LoadInt32(&b.cntOfActiveLevels) <= int32(idx) && atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, int32(b.ptrDownLevels().Len()), int32(idx)+1) {
			cidx := int(atomic.LoadInt32(&b.cntOfActiveLevels)) - 1
			if cidx > 32 || cidx < -32 {
				Log(LogWarn, "invalid cidx ")
			}

			nDownLevel := b.ptrDownLevels()._at(cidx, false)
			nDownLevel.reverse = b.reverse | (uint64(cidx) << (4 * (16 - l)))
			nDownLevel.setLevel(b.childLevel())
			atomic.StoreUint32(&nDownLevel.state, bucketStateInit)

			oBucket := b
			oIdx := cidx

			if onOk != nil {
				Log(LogWarn, "found old fn ")
			}
			nDownLevel.onOkFn = func() {
				oDownLevels := oBucket.ptrDownLevels()
				olen, ocap := oDownLevels.Len(), oDownLevels.Cap()
				if olen > oIdx || ocap <= oIdx { // MENTION: should check ocap <= oIdx
					goto ALREADY
				}
				for {
					if oDownLevels.Len() > oIdx {
						goto EXPAND
					}
					olen, ocap = oDownLevels.Len(), oDownLevels.Cap()
					atomic_util.CompareAndSwapInt(&oDownLevels.len, olen, oIdx+1)
				}

			ALREADY:
				Log(LogDebug, "backet already expand ")

			EXPAND:
				if !atomic.CompareAndSwapUint32(&oBucket.ptrDownLevels().at(oIdx).state, bucketStateInit, bucketStateActive) {
					Log(LogDebug, "fail bucket state change to finish ")
				}
			}
			onOk = nDownLevel.onOkFn

			if !opt.onOk {
				onOk()
				onOk = nil
				break
			}

			b = nDownLevel
			break
		} else if b.ptrDownLevels().Len() <= idx {
			// for debug
			last := b.ptrDownLevels()._at(int(atomic.LoadInt32(&b.cntOfActiveLevels))-1, false)
			if !recoverBucketWithOutInit {
				goto RETRY_SETUP
			}

			if last.isRequireOnOk() {
				last.onOkFn()
			}

			goto RETRY_SETUP
		}

		down := b.ptrDownLevels().at(idx)
		if idx != 0 && atomic.CompareAndSwapUint32(&down.state, bucketStateNone, bucketStateInit) {
			if l != level {
				Log(LogWarn, "not collected already inited")
			}
			down.setLevel(-b.childLevel())
			down.reverse = b.reverse | (uint64(idx) << (4 * (16 - l)))
			if onOk != nil {
				Log(LogWarn, "found old fn ")
			}

			oBucket := b
			oIdx := idx

			onOk = func() {
				if !atomic.CompareAndSwapUint32(&oBucket.ptrDownLevels().at(oIdx).state, bucketStateInit, bucketStateActive) {
					Log(LogWarn, "fail bucket state change to finish ")
				}
			}
			down.onOkFn = onOk

			b = down
			if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
				Log(LogWarn, "already inited")
			}
			break
		} else if idx != 0 && atomic.LoadUint32(&down.state) != bucketStateActive {
			Log(LogWarn, "initializetion is not finished")
			if l == level {
				// the goroutine that won the CAS makes this bucket
				return nil, nil
			}
		}

		if !h.isEmbededItemInBucket && l < level && idx != 0 && atomic.LoadUint32(&down.state) != bucketStateActive {
			bucketNotInits = append(bucketNotInits, down)
		}
		if onOk != nil {
			Log(LogWarn, " skip okfn?")
		}
		b = down
	}
	if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
		h.DumpBucket(logio)
		Log(LogWarn, "already inited")
	}
	// obucketNotInits := bucketNotInits
	// _ = obucketNotInits
	// loncha.Delete(&bucketNotInits, func(i int) bool {
	// 	return b == bucketNotInits[i]
	// })
	// if len(bucketNotInits) > 0 {
	// 	h.setupBcukets(bucketNotInits)
	// }
	if onOk == nil {
		if b.onOkFn != nil {
			onOk = b.onOkFn
		} else {
			Log(LogWarn, "not set reverse?")
		}
	}
	return
}

// init bucket
//  bucketFromPool()            set bucket.reverse, bucket.level < 0 (claimed)
//  makeBucket()/makeBucket2()  init  as bucket elemet  .Init()
//                              init  as level element  .LevelHead.Init()
//    ->addBucket()             find previous bucket
//      -> _InsertBefore()      set dummy.fields
//                              connect dummy to item List
//                              connect bucket to bucket list
//  makeBucket()/makeBucket2()  bucket.level > 0: lookups find the bucket from here
//                              connect bucket to level list

func (h *Map) setupBcukets(buckets []*bucket) {

	emptyListHead := list_head.ListHead{}

	for _, b := range buckets {
		if b.ListHead == emptyListHead {
			b.Init()
		}
		if b.LevelHead == emptyListHead {
			b.LevelHead.Init()
		}
		err := h.addBucket(b)
		if err != nil {
			Log(LogWarn, "fail addBucket() e=%v\n", err)
		}

		if !b.LevelHead.IsSingle() {
			if b.LevelHead.DirectNext() == &b.LevelHead {
				Log(LogWarn, "bucket.LevelHead is pointed to self")
			}
			h.insertOnLevel(b, b.level(), "makeBucket.levelFound", unsafe.Pointer(b))
		}
	}

}
