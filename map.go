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
	list_head "github.com/kazu/lista_encabezado"
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
type Map[K Key[K], V any] struct {
	buckets    [16]bucket[K, V]
	headBucket *list_head.ListHead
	tailBucket *list_head.ListHead

	len          int64
	maxPerBucket int
	head         *elist_head.ListHead
	tail         *elist_head.ListHead

	modeForBucket SearchMode
	// minCapItems is the least capacity of the item pool of a bucket after
	// it grows; see MinCapItems
	minCapItems int
	mu          sync.Mutex
	levels      [16]atomic.Pointer[bucket[K, V]]
	// levelEnds[i] is the sentinel at the end of the list of level i+1: a
	// LevelHead that goes after every bucket of the level is linked before it.
	levelEnds [16]*list_head.ListHead

	pooler *Pool[K, V]

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

type OptHMap[K Key[K], V any] func(*Map[K, V]) OptHMap[K, V]

func MaxPefBucket[K Key[K], V any](max int) OptHMap[K, V] {

	return func(h *Map[K, V]) OptHMap[K, V] {
		prev := h.maxPerBucket
		h.maxPerBucket = max
		return MaxPefBucket[K, V](prev)
	}
}

// MinCapItems sets the least capacity that the item pool of a bucket has
// after it grows, and at least that of its first array; the default is 2. A
// pool grows by doubling, so a larger value saves the growths up to it, for
// the memory of the slots that stay empty.
func MinCapItems[K Key[K], V any](min int) OptHMap[K, V] {
	return func(h *Map[K, V]) OptHMap[K, V] {
		prev := h.minCapItems
		h.minCapItems = min
		return MinCapItems[K, V](prev)
	}
}

func BucketMode[K Key[K], V any](mode SearchMode) OptHMap[K, V] {
	return func(h *Map[K, V]) OptHMap[K, V] {
		prev := h.modeForBucket
		h.modeForBucket = mode
		return BucketMode[K, V](prev)
	}
}

func UsePool[K Key[K], V any](enable bool) OptHMap[K, V] {
	return func(h *Map[K, V]) OptHMap[K, V] {
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&h.pooler))
		if !enable {
			h.pooler = nil
		} else if atomic.LoadPointer(ptr) == nil {
			p := newPool[K, V]()
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(p)) {
				p.startMgr()
			}
		}

		return UsePool[K, V](!enable)
	}
}

func UseEmbeddedPool[K Key[K], V any](enable bool) OptHMap[K, V] {
	return func(h *Map[K, V]) OptHMap[K, V] {
		old := h.isEmbededItemInBucket
		h.isEmbededItemInBucket = enable
		if enable {
			UsePool[K, V](false)(h)
		}

		return UsePool[K, V](old)
	}
}

func (h *Map[K, V]) Options(opts ...OptHMap[K, V]) (previouses []OptHMap[K, V]) {

	for _, fn := range opts {
		previouses = append(previouses, fn(h))
	}
	return

}
func New[K Key[K], V any](opts ...OptHMap[K, V]) *Map[K, V] {

	return NewHMap[K, V](opts...)

}

func NewHMap[K Key[K], V any](opts ...OptHMap[K, V]) *Map[K, V] {
	hmap := &Map[K, V]{
		len:          0,
		maxPerBucket: 32,
		minCapItems:  minCapItem(),
	}

	topBucket := newBucket[K, V]()
	topBucket.InitAsEmpty()
	hmap.tailBucket = topBucket.Next()
	hmap.headBucket = topBucket.Prev()

	hmap.head = &elist_head.ListHead{}
	hmap.tail = &elist_head.ListHead{}
	elist_head.InitAsEmpty(hmap.head, hmap.tail)

	hmap.modeForBucket = NestedSearchForBucket
	hmap.isEmbededItemInBucket = false

	hmap.Options(opts...)

	hmap.initLevels()

	hmap.initBeforeSet()

	// FIXME: remove later
	//hmap.cactchSigBua()

	return hmap
}

func (h *Map[K, V]) cactchSigBua() {

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

// Len returns the number of entries. During concurrent updates it may report
// an intermediate count; after updates finish it returns the exact count.
func (h *Map[K, V]) Len() int {
	return int(atomic.LoadInt64(&h.len))
}

func (h *Map[K, V]) AddLen(inc int64) int64 {
	return atomic.AddInt64(&h.len, inc)
}

func (h *Map[K, V]) initBeforeSet() {
	if !h.notHaveBuckets() {
		return
	}
	btable := newBucket[K, V]()
	btable._level, btable._len = 16, 0
	btable.reverse = ^uint64(0)
	btable.Init()
	btable.LevelHead.Init()

	empty := &btable.dummy
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
				btable._itemPool = &samepleItemPool[K, V]{}
				btable._itemPool.Init()
			}
			btable.setupPool()
			btable._itemPool.reusable = true
			btable._itemPool.minCap = h.minCapItems
			btable._itemPool._init(max(h.maxPerBucket*3/2, h.minCapItems))
		}

		empty = &btable.dummy
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

func (h *Map[K, V]) _update(item *embeddedEntry[K, V], value V) bool {
	if !h.isEmbededItemInBucket || !item.isPoolItem() {
		return h.replaceEntry(item.viewEntry(), value)
	}
	return h.replacePoolEntry(item, value)
}

func (h *Map[K, V]) _validateallbucket() {

	for bucket := bucketFromListHead[K, V](h.headBucket.Next()); bucket != bucket.nextAsB(); bucket = bucket.nextAsB() {
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
func (h *Map[K, V]) TestSet(k, conflict uint64, btable *bucket[K, V], item *Entry[K, V]) bool {
	return h._set(k, conflict, btable.toBase(), &item.embeddedEntry)
}

func (h *Map[K, V]) _set(k, conflict uint64, btable *bucket[K, V], item *embeddedEntry[K, V]) bool {
	return h.setItem(k, conflict, btable, item, false)
}

// setItem links item as _set does. fromUser tells an item that StoreItem got
// from its caller, which the map refuses while it is linked.
func (h *Map[K, V]) setItem(k, conflict uint64, btable *bucket[K, V], item *embeddedEntry[K, V], fromUser bool) bool {

	if !h.isEmbededItemInBucket || fromUser {
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().reverse, 0, bits.Reverse64(k)) {
			Log(LogDebug, "already set reverse")
		}
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().conflict, 0, conflict) {
			Log(LogDebug, "already set conflict")
		}
	}

	h.initBeforeSet()

	var addOpt HMethodOpt[K, V]
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
		btable = newBucket[K, V]()
		//btable.head = h.head.Prev().Next()
	} else {
		addOpt = WithBucket[K, V](btable)
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

	entry, cnt := h.find(btable.head(), func(item *MapHead) bool {
		mHead := item.PtrMapHead()
		return bits.Reverse64(k) <= atomic.LoadUint64(&mHead.reverse)
	})
	_ = cnt

	var tStart *elist_head.ListHead
	if entry != nil {
		if atomic.LoadUint64(&entry.PtrMapHead().reverse) < bits.Reverse64(k) {
			tStart = entry.PtrListHead()
		} else {
			prev := entry.PtrListHead().DirectPrev()
			// The front sentinel has no enclosing map entry.
			if !prev.Empty() && atomic.LoadUint64(&mapheadFromLListHead(prev).reverse) < bits.Reverse64(k) {
				tStart = prev
			}
		}
	}
	if tStart == nil {
		tStart = btable.head()
	}

	stepAt("set.beforeInit", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(tStart))
	atomic.AndUint64((*uint64)(&item.PtrMapHead().state), ^uint64(mapIsDeleted))
	// the marks of an item that the item pool moves stay; add2 links its
	// copy. An item of StoreItem is not cleared: StoreItem found it not
	// linked after it took its mapIsBusy
	if !fromUser && !item.PtrListHead().InitUnmarked() && elist_head.MovedTo(item.PtrListHead()) == nil {
		item.PtrListHead().Init()
	}
	var linked bool
	if fromUser {
		var u userStore
		if addOpt == nil {
			linked = h.add2(tStart, item, forUser[K, V](&u))
		} else {
			linked = h.add2(tStart, item, addOpt, forUser[K, V](&u))
		}
		item.PtrMapHead().releaseBusy()
		if u.linked {
			return false
		}
	} else if addOpt == nil {
		//btable._validateItemsNear()
		linked = h.add2(tStart, item)
		//btable._validateItemsNear()
	} else {
		//btable._validateItemsNear()
		linked = h.add2(tStart, item, addOpt)
		//btable._validateItemsNear()
	}
	if !fromUser && !h.isEmbededItemInBucket {
		item.PtrMapHead().releaseBusy()
	}
	if !linked {
		// the value went into an entry of the key linked meanwhile
		return true
	}
	atomic.AddInt64(&h.len, 1)
	if btable.level() > 0 {
		atomic.AddInt32(&btable._len, 1)
	}
	if !h.isEmbededItemInBucket && btable != nil && int(btable.len()) > h.maxPerBucket {
		h.makeBucket(movedHead(item.PtrListHead()), int(btable.len())/2)
	}

	return true
}

func (h *Map[K, V]) get(key K) (V, bool) {
	e, success := h._get(key.KeyHash())
	if e == nil {
		var zero V
		return zero, success
	}
	return e.Value(), success
}

var Failreverse uint64 = 0

func (h *Map[K, V]) _get(k, conflict uint64) (*embeddedEntry[K, V], bool) {
	return h.getItemMatching(k, conflict, *new(K), false)
}

func (h *Map[K, V]) getItemMatching(k, conflict uint64, key K, byKey bool) (*embeddedEntry[K, V], bool) {
	if h.isEmbededItemInBucket {
		item, _, found := h.getItemWithBucket(k, conflict, key, byKey)
		return item, found
	}
	for {
		e := h.searchItem(k)
		if e == nil {
			return nil, false
		}
		matched, retry := h.matchCopyEntry(e, bits.Reverse64(k), conflict, key, byKey)
		if retry {
			continue
		}
		return matched, matched != nil
	}
}

func (h *Map[K, V]) searchItem(k uint64) *embeddedEntry[K, V] {
	if EnableStats {
		DebugStats[CntOfGet].Add(1)
	}
	e := h.searchKey(k, true)
	if e == nil {
		if atomic.LoadUint64(&Failreverse) == 0 {
			atomic.CompareAndSwapUint64(&Failreverse, 0, bits.Reverse64(k))
		}
		return nil
	}
	if stepEnabled {
		stepAt("get.found", unsafe.Pointer(e.PtrListHead()), nil)
	}
	return e
}

// matchEntry searches neighboring entries with the same reversed hash,
// checking the actual key as well when the caller supplies one.
// Its second result requests a new search when removal or reuse invalidated
// the collision walk; a detached cursor cannot establish that a key is absent.
func (h *Map[K, V]) matchEntry(e *embeddedEntry[K, V], reverse, conflict uint64, key K, byKey, reusable bool) (*embeddedEntry[K, V], bool) {
	if entry := e; !h.isEmbededItemInBucket {
		return h.matchCopyEntry(entry, reverse, conflict, key, byKey)
	}
	mh := e.PtrMapHead()
	if !byKey {
		if matchesLiveHash(mh, reverse, conflict) {
			return e, false
		}
	} else if _, ok := readMatchingEntry[K, V](e, reverse, conflict, key, true, false, reusable); ok {
		return e, false
	}
	return h.matchNeighbors(e, mh, reverse, conflict, key, byKey, reusable)
}

func (h *Map[K, V]) matchNeighbors(e *embeddedEntry[K, V], mh *MapHead, reverse, conflict uint64, key K, byKey, reusable bool) (*embeddedEntry[K, V], bool) {
	state := mapState(atomic.LoadUint64((*uint64)(&mh.state))) &^ mapTransientState
	if atomic.LoadUint64(&mh.reverse) != reverse {
		return nil, true
	}
	retry := state&mapKeyWriting != 0
	matches := func(cur *elist_head.ListHead) (*embeddedEntry[K, V], bool) {
		// The boundary nodes are standalone ListHeads, not embedded MapHeads.
		if cur == h.head || cur == h.tail {
			return nil, false
		}
		mh := mapheadFromLListHead(cur)
		if !linkedEntry(cur) {
			if atomic.LoadUint64(&mh.reverse) == reverse && !mh.IsDummy() {
				retry = true
			}
			return nil, false
		}
		if atomic.LoadUint64(&mh.reverse) != reverse {
			return nil, false
		}
		if !mh.IsIgnored() && atomic.LoadUint64(&mh.conflict) == conflict {
			c := entryHMapFromListHead[K, V](cur)
			if _, ok := readMatchingEntry[K, V](c, reverse, conflict, key, byKey, false, reusable); ok {
				return c, true
			}
		}
		return nil, true
	}
	for cur := e.PtrListHead().DirectNext(); ; cur = cur.DirectNext() {
		c, same := matches(cur)
		if c != nil {
			return c, false
		}
		if !same {
			break
		}
	}
	for cur := e.PtrListHead().DirectPrev(); ; cur = cur.DirectPrev() {
		c, same := matches(cur)
		if c != nil {
			return c, false
		}
		if !same {
			break
		}
	}
	return nil, retry || mapState(atomic.LoadUint64((*uint64)(&mh.state)))&^mapTransientState != state
}

func (h *Map[K, V]) getWithBucket(k, conflict uint64) (*embeddedEntry[K, V], *bucket[K, V], bool) {
	return h.getItemWithBucket(k, conflict, *new(K), false)
}

func (h *Map[K, V]) getItemWithBucket(k, conflict uint64, key K, byKey bool) (*embeddedEntry[K, V], *bucket[K, V], bool) {
	item, bucket, _, found := h.lookupItem[noDummyTrace](k, conflict, key, byKey)
	return item, bucket, found
}

func (h *Map[K, V]) lookupItem[T dummyTrace](k, conflict uint64, key K, byKey bool) (*embeddedEntry[K, V], *bucket[K, V], *MapHead, bool) {
	var trace T
	if EnableStats {
		DebugStats[CntOfGet].Add(1)
	}
	for {
		var bucket *bucket[K, V]
		var reverse uint64
		var e *embeddedEntry[K, V]
		var dummy *MapHead
		var pool *samepleItemPool[K, V]
		var version uint64

		if h.isEmbededItemInBucket {
			reverse = bits.Reverse64(k)
			bucket = h.findBucket(reverse)
			e, pool, version = h.bsearchPool(bucket, reverse, true)

		} else {
			bucket, reverse = h.searchBucket4update(k)
			if !h.isEmbededItemInBucket && bucket.head() == nil {
				bucket, reverse = h.searchBucket4update(k)
			}
			if len(trace) != 0 {
				e = h.searchCopyBucket[saveDummyTrace](bucket, reverse, true, &dummy)
			} else {
				e = h.searchBybucket(bucket, reverse, true)
			}
		}

		if e == nil {
			if pool != nil && pool.arrayState.Load() != version {
				continue
			}
			if atomic.LoadUint64(&Failreverse) == 0 {
				atomic.CompareAndSwapUint64(&Failreverse, 0, bits.Reverse64(k))
			}
			return nil, bucket, dummy, false
		}
		var retry bool
		if len(trace) != 0 && !h.isEmbededItemInBucket {
			e, retry = h.matchCopyEntryWithDummy(e, bits.Reverse64(k), conflict, key, byKey, &dummy)
		} else {
			if stepEnabled && h.isEmbededItemInBucket {
				stepAt("lookup.pool.candidate", unsafe.Pointer(e.PtrListHead()), nil)
			}
			e, retry = h.matchEntry(e, bits.Reverse64(k), conflict, key, byKey, h.isEmbededItemInBucket)
		}
		if retry || (pool != nil && pool.arrayState.Load() != version) {
			continue
		}
		if e == nil {
			return nil, bucket, dummy, false
		}

		return e, bucket, dummy, true
	}

}

func (h *Map[K, V]) notHaveBuckets() bool {
	return h.tailBucket.Next().Prev().Empty()
}

func levelMask(level int) (mask uint64) {
	mask = 0
	for i := 0; i < level; i++ {
		mask = (mask << 4) | 0xf
	}
	return
}

func (h *Map[K, V]) searchBucket(k uint64) (result *bucket[K, V]) {
	cnt := 0

	idx := bits.Reverse64(k) >> (4 * 15)
	for cur := &h.buckets[idx].ListHead; !cur.Empty(); cur = cur.DirectNext() {
		bcur := bucketFromListHead[K, V](cur)
		if bits.Reverse64(k) > bcur.reverse {
			return bcur
		}
		cnt++
	}
	return
}

// Get returns the value for key, or false if none is found. Concurrent removal
// of a traversal node may also make the lookup fail while key remains present.
func (h *Map[K, V]) Get(key K) (value V, ok bool) {
	hash, conflict := key.KeyHash()
	return h.getValueMatching(hash, conflict, key, true)
}

// GetByHash returns the value of one entry matching the hash pair.
// As with Get, concurrent removal may make the lookup fail.
func (h *Map[K, V]) GetByHash(hash, conflict uint64) (value V, ok bool) {
	return h.getValueMatching(hash, conflict, *new(K), false)
}

// LoadItem returns the entry for key, or false if not found.
// It panics with UseEmbeddedPool; use Get to read a value instead.
// Updates may replace the entry. Change entries through the map's methods.
func (h *Map[K, V]) LoadItem(key K) (item *Entry[K, V], success bool) {
	h.checkEntryAccess()
	hash, conflict := key.KeyHash()
	entry, _, success := h.getItemWithBucket(hash, conflict, key, true)
	return entry.viewEntry(), success
}

// LoadItemByHash returns one entry matching the hash pair of KeyToHash.
// It panics with UseEmbeddedPool; use GetByHash to read a value instead.
// Updates may replace the entry, as described in LoadItem.
func (h *Map[K, V]) LoadItemByHash(k uint64, conflict uint64) (item *Entry[K, V], success bool) {
	h.checkEntryAccess()
	entry, success := h._get(k, conflict)
	return entry.viewEntry(), success
}

func (h *Map[K, V]) checkEntryAccess() {
	if h.isEmbededItemInBucket {
		panic("skiplistmap: entry access is unavailable with UseEmbeddedPool; use Get or Range")
	}
}

func (h *Map[K, V]) loadItem(k uint64, conflict uint64, key K) (item *embeddedEntry[K, V], bucket *bucket[K, V], lock *trylock.Mutex, found bool) {

	for {
		hash, conflict := key.KeyHash()
		item, bucket, found = h.getItemWithBucket(hash, conflict, key, true)
		if stepEnabled && found {
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
// when item is still live with the requested key; pool slots must belong to
// the current array. A nil item validates that the key is still absent.
func (h *Map[K, V]) lockFoundItem(key K, item *embeddedEntry[K, V], bucket *bucket[K, V]) (*trylock.Mutex, bool) {
	owner := bucket.toBase()
	mu := &owner.muPool
	if !mu.TryLock() {
		return nil, false
	}
	hash, conflict := key.KeyHash()
	reverse := bits.Reverse64(hash)
	if h.findBucket(reverse).toBase() == owner {
		if item == nil {
			if _, _, found := h.getItemWithBucket(hash, conflict, key, true); !found {
				return mu, true
			}
		} else if !item.isPoolItem() {
			if _, valid := readMatchingEntry[K, V](item, reverse, conflict, key, true, false, false); valid && !item.ListHead.IsMarked() {
				return mu, true
			}
		}
		// The lock keeps this array and its slots stable. Validate the original
		// slot directly, including its current key, instead of searching again.
		if sample := item; sample != nil && sample.isPoolItem() {
			items := owner.itemPool().ptrItems()
			offset := uintptr(unsafe.Pointer(sample)) - uintptr(atomic.LoadPointer(&items.data))
			if offset < uintptr(items.Len())*items.stride && offset%items.stride == 0 {
				if _, valid := readMatchingEntry[K, V](item, reverse, conflict, key, true, false, true); valid {
					return mu, true
				}
			}
		}
	}
	mu.Unlock()
	return nil, false
}

func (h *Map[K, V]) _loadItem(k uint64, conflict uint64, key K) (*embeddedEntry[K, V], *bucket[K, V], bool) {
	hash, keyConflict := key.KeyHash()
	return h.getItemWithBucket(hash, keyConflict, key, true)
}

var madeBucket int32 = 0

// Set ... set the value for a key
//
// When Set adds a new key, the item that holds it lives in the map's item
// pool, whose array keeps it reachable for the GC. A map without
// UseEmbeddedPool creates the pool on the first such Set if it has none. For
// a key already present, Set publishes a replacement entry with the new value.
func (h *Map[K, V]) Set(key K, value V) bool {
	atomic.StoreInt32(&madeBucket, 0)

	var item *embeddedEntry[K, V]
	var bucket *bucket[K, V]
	var found bool
	k, conflict := key.KeyHash()

	if !h.isEmbededItemInBucket {
		item, bucket, found = h.getItemWithBucket(k, conflict, key, true)
		if found {
			return h._update(item, value)
		}
	} else {
		// the lookup runs below, once, under the lock of the pool
		bucket = h.findBucket(bits.Reverse64(k))
	}

	var s *embeddedEntry[K, V]
	useDump := false

	if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&h.pooler))) == nil && !h.isEmbededItemInBucket {
		UsePool[K, V](true)(h)
	}
	if !h.isEmbededItemInBucket && bucket.head().Empty() {
		bucket.head()
		h._loadItem(0, 0, key)
		Log(LogDebug, "empty is invalid")
	}

	if h.isEmbededItemInBucket {
		var nPool *samepleItemPool[K, V]

		// Hold the lock of the pool from the lookup to the store of the
		// value or to the link, so that the item found is not purged and
		// its slot reused before the store, and two goroutines inserting
		// the same key cannot both miss and both insert.
		var mu *trylock.Mutex
		for {
			mu = &bucket.toBase().muPool
			stepAt("set.newKeyLock", unsafe.Pointer(bucket), unsafe.Pointer(mu))
			mu.Lock()
			nb := bucket
			item, nb, found = h.getItemWithBucket(k, conflict, key, true)
			if nb.toBase() == bucket.toBase() {
				break
			}
			mu.Unlock()
			bucket = nb
		}
		defer mu.Unlock()
		if found {
			if stepEnabled {
				stepAt("set.updateLocked", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(bucket))
			}
			return h._update(item, value)
		}

		//lastgets = nil
		oPool := bucket.itemPool()
		item, nPool, _ := oPool.getWithFn(bits.Reverse64(k), nil)

		s = item
		if nPool != nil {
			bucket.setItemPool(nPool)
		}
		if stepEnabled {
			stepAt("set.slotTaken", unsafe.Pointer(s.PtrListHead()), unsafe.Pointer(bucket))
		}

		// s.PtrMapHead().reverse = bits.Reverse64(k)
		// s.PtrMapHead().conflict = conflict
		// the pool wrote the reverse when it handed out the slot
		if r := bits.Reverse64(k); atomic.LoadUint64(&s.PtrMapHead().reverse) != r &&
			!atomic.CompareAndSwapUint64(&s.PtrMapHead().reverse, 0, r) {
			Log(LogDebug, "already set reverse")
		}
		if !atomic.CompareAndSwapUint64(&item.PtrMapHead().conflict, 0, conflict) {
			Log(LogDebug, "already set conflict")
		}
		stepAt("set.identity", unsafe.Pointer(s.PtrListHead()), nil)
	} else {
		var pooled *embeddedEntry[K, V]
		var held sync.Locker
		var ready sync.WaitGroup
		ready.Add(1)
		pooler := (*Pool[K, V])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&h.pooler))))
		pooler.Get(bits.Reverse64(k), func(item MapItem[K, V], lock sync.Locker) {
			pooled, held = &item.embeddedEntry, lock
			ready.Done()
		})
		ready.Wait()
		s = pooled
		if held != nil {
			defer held.Unlock()
		}
		if s != nil && !s.IsSingle() {
			Log(LogWarn, "get not single entry?")
		}
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

	s.storeTypedKeyValue(key, value)
	st := mapIsPoolItem
	if !h.isEmbededItemInBucket {
		st |= mapIsBusy
	}
	atomic.OrUint64((*uint64)(&s.PtrMapHead().state), uint64(st))

	if h.isEmbededItemInBucket {
		return h._set(k, conflict, bucket.toBase(), s)
	}
	if !s.IsSingle() {
		Log(LogWarn, "is not single")
	}
	return h._set(k, conflict, bucket, s)
}

// StoreItem ... set key/value item with embedded-linked-list
//
// StoreItem links item itself into the map and does not copy it. The map
// holds only offsets to item, which the GC does not follow, so the caller
// must keep item reachable while it is linked: until Purge of its key
// returns true, or until the caller stops using the map. Delete leaves item
// linked, and a Purge after Delete does not find it: keep an item that
// Delete removed reachable as long as the map is used. A Delete that finds
// item while a StoreItem of item is still linking it returns false without
// UseEmbeddedPool; embedded writers serialize through the bucket lock.
// Otherwise the map links freed memory (a dangling reference). The map never
// moves item. If the key is already present, its value is updated in the map,
// and item is not linked. StoreItem returns false for an
// item still linked, in this map or in another. An entry deleted or retired
// by replacement cannot be stored again, even after Purge or link initialization;
// use Entry.Copy or StoreItemOrCopy to store its data in a new entry.
// It returns false also while
// another StoreItem, a Delete or a Purge of item is running, and for an
// item owned by the map's item pool, such as an item stored by Set.
// Entry retrieval APIs are unavailable with UseEmbeddedPool (see LoadItem).
// With UseEmbeddedPool, external entries coexist with pool entries and do
// not move when the pool grows. Updates publish copies retained by the
// original external entry, as they do without UseEmbeddedPool.
func (h *Map[K, V]) StoreItem(item *Entry[K, V]) bool {
	ok, _ := h.storeItem(&item.embeddedEntry)
	return ok
}

// StoreItemOrCopy stores item as StoreItem does. Only a refusal due to deletion
// or retirement causes it to copy item and try StoreItem with the copy. Pool
// entries and busy entries are refused without copying.
// It returns the entry passed to the successful StoreItem, or nil on failure.
// Keep the returned entry reachable as required by StoreItem, and keep any
// previously stored root alive for its existing map too. If the key already
// exists, StoreItem updates that entry instead of linking the returned entry.
func (h *Map[K, V]) StoreItemOrCopy(item *Entry[K, V]) (*Entry[K, V], bool) {
	return h.storeItemOrCopy(&item.embeddedEntry)
}

func (h *Map[K, V]) storeItemOrCopy(item *embeddedEntry[K, V]) (*Entry[K, V], bool) {
	ok, retired := h.storeItem(item)
	if retired {
		item = &item.Copy().embeddedEntry
		ok, _ = h.storeItem(item)
	}
	if !ok {
		return nil, false
	}
	return item.viewEntry(), true
}

func (h *Map[K, V]) storeItem(item *embeddedEntry[K, V]) (ok, retired bool) {
	if item.PtrMapHead().isPoolItem() {
		return false, false
	}
	if state := mapState(atomic.LoadUint64((*uint64)(&item.state))); state&mapIsRetired != 0 {
		return false, state&mapIsBusy == 0
	}
	// an item still linked is refused also when its key is present, where
	// the value would go into the item found
	if !item.PtrListHead().IsSingle() {
		return false, false
	}
	stepAt("storeItem.checked", unsafe.Pointer(item.PtrListHead()), nil)
	if !item.PtrMapHead().claimBusy() {
		return false, false
	}
	if mapState(atomic.LoadUint64((*uint64)(&item.state)))&mapIsRetired != 0 {
		item.PtrMapHead().releaseBusy()
		return false, true
	}
	if !item.PtrListHead().IsSingle() {
		item.PtrMapHead().releaseBusy()
		return false, false
	}
	k, conflict := item.KeyHash()

	var oitem *embeddedEntry[K, V]
	var bucket *bucket[K, V]
	var found bool
	if h.isEmbededItemInBucket {
		var lock *trylock.Mutex
		oitem, bucket, lock, found = h.loadItem(k, conflict, item.Key())
		defer lock.Unlock()
	} else {
		oitem, bucket, found = h.getItemWithBucket(k, conflict, item.Key(), true)
	}
	if found {
		ok := h._update(oitem, item.Value())
		item.PtrMapHead().releaseBusy()
		return ok, false
	}
	return h.setItem(k, conflict, bucket, item, true), false
}

func (h *Map[K, V]) eachEntry(start *elist_head.ListHead, fn func(*entryHMap[K, V])) {
	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead[K, V](cur)
		if e == nil || e.IsIgnored() {
			continue
		}
		fn(e)
	}
	return
}

func (h *Map[K, V]) each(start *elist_head.ListHead, fn func(key K, value V)) {

	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead[K, V](cur)
		if e != nil {
			fn(e.Key(), e.Value())
		}
	}
	return
}

// must renename to find
// walkStart returns the node that a walk from anchor starts at: the first
// node after the head of the list, or the anchor itself, or the node that
// took its place when it was deleted.
func walkStart(anchor *elist_head.ListHead) *elist_head.ListHead {
	if anchor.Empty() {
		return anchor.Next()
	}
	return anchor.Prev().Next()
}

// walkEnd returns the node that the walk of find from start ends at: the tail
// of the list, or the last node of a block that a move took out of the list.
func (h *Map[K, V]) walkEnd(start *elist_head.ListHead) *elist_head.ListHead {
	cur := start
	for cur != cur.Next() {
		cur = cur.Next()
	}
	return cur
}

func (h *Map[K, V]) find(start *elist_head.ListHead, cond func(*MapHead) bool) (result *MapHead, cnt int) {
	stepAt("find.begin", unsafe.Pointer(start), nil)
	if start.Empty() {
		return
	}
	for cur := start; cur != cur.Next(); cur = cur.Next() {
		head := mapheadFromLListHead(cur)
		if cond(head) {
			return head, cnt
		}
		cnt++
	}
	return nil, cnt
}

func (h *Map[K, V]) makeBucket(ocur *elist_head.ListHead, back int) (err error) {

	stepAt("makeBucket.begin", unsafe.Pointer(ocur), nil)
	enableDumpBucket := false

	cur := ocur
	cur = cur.Prev()

	e := mapheadFromLListHead(cur)
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
		e := mapheadFromLListHead(cur)
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

type hmapMethod[K Key[K], V any] struct {
	bucket *bucket[K, V]

	// anchor is a node that stays on the list, which the walk of linkEntry
	// starts from again when the node it started from left the list
	anchor *elist_head.ListHead

	// user is set for an item of StoreItem: add2 does not take it out of
	// the list that it is found linked into, and reports that in user instead.
	user *userStore
}

// userStore is what add2 reports for an item of StoreItem, besides whether it
// stored the item.
type userStore struct {
	// linked: add2 found the item linked, and left it
	linked bool
}

type HMethodOpt[K Key[K], V any] func(*hmapMethod[K, V])

func WithBucket[K Key[K], V any](b *bucket[K, V]) func(*hmapMethod[K, V]) {

	return func(conf *hmapMethod[K, V]) {
		conf.bucket = b
	}
}

// withAnchor gives linkEntry a node that stays on the list, to start its walk
// from again when the node it started from left the list.
func withAnchor[K Key[K], V any](anchor *elist_head.ListHead) HMethodOpt[K, V] {
	return func(conf *hmapMethod[K, V]) {
		conf.anchor = anchor
	}
}

// forUser makes add2 store an item of StoreItem, and report in u what it did
// when it did not link the item.
func forUser[K Key[K], V any](u *userStore) HMethodOpt[K, V] {
	return func(conf *hmapMethod[K, V]) {
		conf.user = u
	}
}

func (h *Map[K, V]) add2(start *elist_head.ListHead, e *embeddedEntry[K, V], opts ...HMethodOpt[K, V]) bool {
	return h.linkEntry(start, e.PtrMapHead(), e, opts...)
}

func (h *Map[K, V]) linkEntry(start *elist_head.ListHead, node *MapHead, item *embeddedEntry[K, V], opts ...HMethodOpt[K, V]) bool {
	var opt *hmapMethod[K, V]
	if len(opts) > 0 {
		opt = &hmapMethod[K, V]{}
		for _, fn := range opts {
			fn(opt)
		}
	}

	cnt := 0

RETRY:
	// the item pool moves item to a larger array before item is linked: the copy
	// is linked instead
	if item != nil {
		item = movedEntry[K, V](item)
		node = item.PtrMapHead()
	}
	if opt != nil && opt.user != nil && !node.PtrListHead().IsSingle() {
		opt.user.linked = true
		return false
	}
	if start.IsMarked() || start.Empty() {
		// start was deleted, taken out by Purge, replaced by its copy by an
		// expand of its pool, or moved with its block by a slide; find the
		// position from the anchor, from the dummy of the bucket, or from
		// the live node before start
		switch {
		case opt != nil && opt.anchor != nil:
			start = walkStart(opt.anchor)
		case opt != nil && opt.bucket != nil:
			start = opt.bucket.head()
		default:
			start = elist_head.PrevNoM(start)
		}
	}
	pos, _ := h.find(start, func(ehead *MapHead) bool {
		cnt++
		if !node.PtrListHead().IsSingle() {
			Log(LogWarn, "add2: element for insertion  is not single ")
		}
		nodeReverse := atomic.LoadUint64(&node.reverse)
		headReverse := atomic.LoadUint64(&ehead.PtrMapHead().reverse)
		return nodeReverse < headReverse || (node.IsDummy() && nodeReverse == headReverse)
	})
	if stepEnabled && pos != nil {
		stepAt("add2.found", unsafe.Pointer(node.PtrListHead()), unsafe.Pointer(pos.PtrListHead()))
	}
	if !node.PtrListHead().IsSingle() {

		// rev := node.reverse
		// idx := (rev >> (4 * 15) % cntOfPoolMgr)
		// p := samepleItemPoolFromListHead(h.pooler.itemPool[idx].Next())
		// _ = p
		if IsInfo() {
			pools := h.allpools()
			_ = pools
			node.PtrListHead().IsSingle()
		}
		Log(LogWarn, "add2: element for insertion  is not single ")
	}

	if pos != nil {
		if !node.PtrListHead().IsSingle() {
			if opt != nil && opt.user != nil {
				goto RETRY
			}
			Log(LogWarn, "add2: element for insertion  is not single ")
			err := node.PtrListHead().MarkForDelete()
			if err == elist_head.ErrMoved {
				goto RETRY
			}
			if err != nil {
				Log(LogError, "fail delete")
			}
			node.PtrListHead().Init()
			if elist_head.MovedTo(node.PtrListHead()) != nil {
				goto RETRY
			}
		}

		if item != nil && h.storeIntoSameKey(pos.PtrListHead(), item) {
			return false
		}
		if err := insertInOrder[K, V](pos.PtrListHead(), node.PtrListHead(), item); err != nil {
			runtime.Gosched()
			goto RETRY
		}
		if opt == nil || opt.bucket == nil {
			return true
		}
		btable := opt.bucket
		if btable == nil || node.IsIgnored() || int(btable.len()) <= h.maxPerBucket {
			return true
		}

		// FIXME: not run on !h.isEmbededItemInBucket
		if !h.isEmbededItemInBucket {
			//h.makeBucket(node.PtrListHead(), int(btable.len())/2)
		} else {
			h.makeBucket2(btable)
		}

		return true
	}
	// no position: the walk from start reached the tail of the list, or the
	// last node of a block that a move took out of the list while the walk
	// was in it; then the walk starts again from a node that stays on the list
	if end := h.walkEnd(start); end != h.tail {
		switch {
		case opt != nil && opt.anchor != nil:
			start = walkStart(opt.anchor)
		case opt != nil && opt.bucket != nil:
			start = opt.bucket.head()
		default:
			start = elist_head.PrevNoM(start)
		}
		runtime.Gosched()
		goto RETRY
	}
	if opt != nil && opt.bucket != nil && opt.bucket.entry(h) != nil {
		// pos, _ = h.find(start, func(ehead HMapEntry) bool {
		// 	return node.reverse < ehead.PtrMapHead().reverse || (node.IsDummy() && node.reverse == ehead.PtrMapHead().reverse)
		// })
		nextE := nextMapHead(opt.bucket.entry(h))
		if nextE.PtrMapHead().reverse <= EmptyMapHead.fromListHead(opt.bucket.head()).reverse {
			Log(LogWarn, "map.add2() re-try get target")
			mHead := EmptyMapHead.fromListHead(opt.bucket.head().Next())
			_ = mHead
			nextE = opt.bucket.prevAsB().entry(h)
		}

		stepAt("add2.bucketInsert", unsafe.Pointer(node.PtrListHead()), unsafe.Pointer(nextE.PtrListHead()))
		if item != nil && h.storeIntoSameKey(nextE.PtrListHead(), item) {
			return false
		}
		if err := insertInOrder[K, V](nextE.PtrListHead(), node.PtrListHead(), item); err == nil {
			return true
		}
		// the entry after the dummy of the bucket is not a place for item;
		// no entry comes after item, so item goes just before the last one
	}
	pos, _ = h.find(start, func(ehead *MapHead) bool {
		cnt++
		return node.reverse < ehead.PtrMapHead().reverse || (node.IsDummy() && node.reverse == ehead.PtrMapHead().reverse)
	})
	if pos != nil {
		goto RETRY
	}

	if stepEnabled {
		stepAt("add2.tailInsert", unsafe.Pointer(node.PtrListHead()), unsafe.Pointer(h.tail.Prev()))
	}
	if item != nil && h.storeIntoSameKey(h.tail.Prev(), item) {
		return false
	}
	if err := insertInOrder[K, V](h.tail.Prev(), node.PtrListHead(), item); err != nil {
		runtime.Gosched()
		goto RETRY
	}
	return true
}

// movedEntry returns the copy of e with the data of e, when the item pool
// moves or moved the array of e to a larger one, and e otherwise. The caller
// owns e, which is not linked; the copy is not linked, and nobody else links
// it.
func movedEntry[K Key[K], V any](e *embeddedEntry[K, V]) *embeddedEntry[K, V] {
	for {
		// a move marks the links of e before it takes e as linked or not
		if !e.PtrListHead().IsMarked() {
			return e
		}
		head := elist_head.MovedTo(e.PtrListHead())
		if head == nil {
			return e
		}
		s := e
		c := entryHMapFromListHead[K, V](head)
		c.copyFrom(s)
		e = c
	}
}

// movedHead returns the node that the list holds for head: head, or its
// copy when the item pool moves or moved the array of head to a larger one.
func movedHead(head *elist_head.ListHead) *elist_head.ListHead {
	for {
		c := elist_head.MovedTo(head)
		if c == nil {
			return head
		}
		head = c
	}
}

// storeIntoSameKey stores the value of e into a live entry of the key of e
// among the entries of the reverse of e just before right, which another
// store linked after the lookup of e missed the key. It reports whether it
// did.
func (h *Map[K, V]) storeIntoSameKey(right *elist_head.ListHead, e *embeddedEntry[K, V]) bool {
	left := right.DirectPrev()
	if left == right {
		return false
	}
	same := linkedSameKey[K, V](mapheadFromLListHead(left), e.PtrMapHead(), e)
	if same == nil {
		return false
	}
	h._update(entryHMapFromListHead[K, V](same.PtrListHead()), e.Value())
	return true
}

func (h *Map[K, V]) BackBucket() (bCur *bucket[K, V]) {

	for cur := h.headBucket.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		bCur = bucketFromListHead[K, V](cur)
	}
	return
}

func (h *Map[K, V]) toFrontBucket(bucket *bucket[K, V]) (result *bucket[K, V]) {

	for cur := bucket.PtrListHead(); !cur.Empty(); cur = cur.DirectPrev() {
		result = bucketFromListHead[K, V](cur)
	}
	return
}

func (h *Map[K, V]) toBackBucket(bucket *bucket[K, V]) (result *bucket[K, V]) {

	for cur := bucket.PtrListHead(); !cur.Empty(); cur = cur.DirectNext() {
		result = bucketFromListHead[K, V](cur)
	}
	return
}

func (h *Map[K, V]) DumpBucket(w io.Writer) {
	var b strings.Builder

	for cur := h.headBucket.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		btable := bucketFromListHead[K, V](cur)
		fmt.Fprintf(&b, "  bucket{reverse: 0x%16x, len: %d, start: %p, level{%d, cur: %p, prev: %p next: %p} down: %d}\n",
			btable.reverse, btable.len(), btable.head, btable.level(), &btable.LevelHead, btable.LevelHead.DirectPrev(), btable.LevelHead.DirectNext(), btable.ptrDownLevels().Len())
	}
	if w == nil {
		os.Stdout.WriteString(b.String())
		return
	}
	w.Write([]byte(b.String()))

}

func (h *Map[K, V]) DumpBucketPerLevel(w io.Writer) {
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
			cBucket = bucketFromLevelHead[K, V](cur)
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

func (h *Map[K, V]) DumpEntry(w io.Writer) {
	defer runtime.KeepAlive(h)
	var b strings.Builder

	for cur := h.head.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		//var e HMapEntry
		//e = e.HmapEntryFromListHead(cur)
		mhead := EmptyMapHead.FromListHead(cur)
		e := fromMapHead[K, V](mhead)

		var ekey K
		if e != nil {
			ekey = e.Key()
		}
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

func (h *Map[K, V]) _InsertBefore(tBtable *list_head.ListHead, nBtable *bucket[K, V]) {

	stepAt("insertBucket.begin", unsafe.Pointer(nBtable), nil)
	empty := &nBtable.dummy
	empty.reverse, empty.conflict = nBtable.reverse, 0
	empty.PtrMapHead().state |= mapIsDummy
	empty.Init()
	anchor := h.head
	if !tBtable.Empty() {
		anchor = bucketFromListHead[K, V](tBtable).head()
	}
	h.linkEntry(walkStart(anchor), empty, nil, withAnchor[K, V](anchor))
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

func (h *Map[K, V]) addBucket(nBtable *bucket[K, V]) error {

	pos := h.bucketInsertPos(nBtable.reverse)
	if !pos.Empty() && bucketFromListHead[K, V](pos).reverse == nBtable.reverse {
		return ErrBucketAlreadyExit
	}
	h._InsertBefore(pos, nBtable)
	return nil
}

// bucketInsertPos returns the node of the list of buckets, which is in
// descending order of reverse, that a bucket of reverse goes before: the
// first bucket whose reverse is not larger, or the end of the list.
func (h *Map[K, V]) bucketInsertPos(reverse uint64) *list_head.ListHead {
	pos := h.headBucket.Prev().Next()
	if h.isEmbededItemInBucket {
		if b := h.findBucket(reverse); b != nil {
			// Embedded children become searchable after linking. The zero
			// child aliases its parent rather than belonging to this list.
			pos = &b.toBase().ListHead
			// A linked child can still be absent from the hierarchy search
			// while its level is negative. Include it via the actual links.
			for prev := pos.Prev(); !prev.Empty() && bucketFromListHead[K, V](prev).reverse <= reverse; prev = pos.Prev() {
				pos = prev
			}
		}
	}
	for !pos.Empty() && bucketFromListHead[K, V](pos).reverse > reverse {
		pos = pos.Next()
	}
	return pos
}

// instant function remote later
func (h *Map[K, V]) HeadBucket() *list_head.ListHead {
	return h.headBucket
}

// linkBucket links nBtable into the list of buckets before pos. It links it
// only while the bucket before pos is larger, and finds the place again when
// another bucket was linked there meanwhile; it leaves the list as it is when
// a bucket of the same reverse is linked by then.
func (h *Map[K, V]) linkBucket(pos *list_head.ListHead, nBtable *bucket[K, V]) {
	for retry := 0; ; retry++ {
		if retry > 0 {
			runtime.Gosched()
			pos = h.bucketInsertPos(nBtable.reverse)
		}
		if !pos.Empty() && bucketFromListHead[K, V](pos).reverse == nBtable.reverse {
			return
		}
		err := pos.TryInsertBefore(&nBtable.ListHead, func(prev *list_head.ListHead) bool {
			return prev.Empty() || bucketFromListHead[K, V](prev).reverse > nBtable.reverse
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
func (h *Map[K, V]) insertOnLevel(b *bucket[K, V], level int32, point string, at unsafe.Pointer) {
	for retry := 0; ; retry++ {
		if retry > 0 {
			runtime.Gosched()
		}
		var pos *list_head.ListHead
		if retry == 0 {
			pos = h.nextOnLevelOf(b, level)
		}
		if pos == nil {
			pos = h.levelBucket(level).LevelHead.Next()
			for !pos.Empty() && bucketFromLevelHead[K, V](pos).reverse > b.reverse {
				pos = pos.Next()
			}
		}
		if !pos.Empty() && bucketFromLevelHead[K, V](pos).reverse == b.reverse {
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
			return prev.Empty() || bucketFromLevelHead[K, V](prev).reverse > b.reverse
		})
		if err == nil {
			return
		}
	}
}

// nextOnLevelOf returns the node of the list of level of the first bucket of
// level among the buckets that follow b in the list of buckets, which is in
// the same order as the list of level, so that the walk of the list of level
// from its start is not needed. The first bucket of the down levels of a
// bucket has the reverse of that bucket and is not in the list of buckets: it
// is looked for below the buckets that follow b, and the walk of such a b
// starts from its parent. It returns nil when no bucket of level is among the
// ones that follow b closely.
func (h *Map[K, V]) nextOnLevelOf(b *bucket[K, V], level int32) *list_head.ListHead {
	const near = 64
	cur := b
	if h.isEmbededItemInBucket {
		// the parent of a first down level can be a first down level itself,
		// which is not on the list of buckets either: walk from the nearest
		// ancestor that is
		for cur.nextAsB() == cur && cur._parent != nil {
			cur = cur._parent
		}
	} else if cur.nextAsB() == cur && b._parent != nil {
		cur = b._parent
	}
	for i := 0; i < near; i++ {
		next := cur.nextAsB()
		if next == cur {
			if h.isEmbededItemInBucket {
				// no bucket follows cur, so no bucket of level follows b: b
				// goes at the end of the list of level
				return h.levelEnds[level-1]
			}
			return nil
		}
		if next.reverse > b.reverse {
			return nil
		}
		for down := next; down != nil && down != b; down = down.ptrDownLevels().at(0) {
			if l := down.level(); l == level {
				// a split puts its bucket on the list of buckets before it
				// links the LevelHead, which lies between sentinels of its
				// own until then; the start of the list of level is the
				// only sentinel before a LevelHead on that list
				if p := down.LevelHead.DirectPrev(); p.Empty() && p != &h.levelBucket(level).LevelHead {
					return nil
				}
				return &down.LevelHead
			} else if l <= 0 || l > level {
				break
			}
		}
		cur = next
	}
	return nil
}

func (h *Map[K, V]) initLevels() {

	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.levels {
		b := newBucket[K, V]()
		b.setLevel(int32(i) + 1)
		b.LevelHead.InitAsEmpty()
		h.levelEnds[i] = b.LevelHead.DirectNext()
		h.levels[i].Store(b)
	}
}

func (h *Map[K, V]) setLevel(level int32, b *bucket[K, V]) bool {

	return false
}

func (h *Map[K, V]) levelBucket(level int32) (b *bucket[K, V]) {
	b = h.levels[level-1].Load()

	return b
}

func (h *Map[K, V]) isEmptyBylevel(level int32) bool {
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
	CntSearchBucket    statKey = 1
	CntLevelBucket     statKey = 2
	CntSearchEntry     statKey = 3
	CntReverseSearch   statKey = 4
	CntOfGet           statKey = 5
	CntPoolSlide       statKey = 6
	CntPoolInsertAlloc statKey = 7
	CntPoolHoleSlide   statKey = 8
	CntPoolExpand      statKey = 9
	statCount          statKey = 10
)

func nextNoCheck[K Key[K], V any](e *embeddedEntry[K, V]) *embeddedEntry[K, V] {
	return e.Next()
}

func prevNoCheck[K Key[K], V any](e *embeddedEntry[K, V]) *embeddedEntry[K, V] {
	return e.Prev()
}

func nextMapHead(e *MapHead) *MapHead {
	start := e.PtrListHead()
	if !start.DirectNext().Empty() {
		start = start.DirectNext()
	}
	if !start.Empty() {
		return mapheadFromLListHead(start)
	}
	return nil
}

func prevAsE[K Key[K], V any](e *embeddedEntry[K, V]) *embeddedEntry[K, V] {
	start := e.PtrListHead()
	if !start.DirectPrev().Empty() {
		start = start.DirectPrev()
	}
	if !start.Empty() {
		return entryHMapFromListHead[K, V](start)
	}
	return nil
}

func init() {
	// lista reads this for every list, so it is set once, before any
	// goroutine uses a map
	list_head.MODE_CONCURRENT = true
}

// SearchKey returns one live value matching the primary hash k, or false if
// none is found or concurrent removal invalidates the candidate.
// Use Get or GetByHash when the key or conflict hash is known.
func (h *Map[K, V]) SearchKey(k uint64) (V, bool) {
	defer runtime.KeepAlive(h)
	e := h.searchKey(k, true)
	if e == nil {
		var zero V
		return zero, false
	}
	return h.readEntryValue(e, bits.Reverse64(k))
}

func (h *Map[K, V]) searchKey(k uint64, ignoreBucketEnry bool) *embeddedEntry[K, V] {
	if h.isEmbededItemInBucket {
		return h.searchKeyFromEmbeddedPool(k, ignoreBucketEnry)
	}
	return h.searchBybucket(h.searchBucket4Key4(k, ignoreBucketEnry))
}

func (h *Map[K, V]) topLevelBucket(reverse uint64) *bucket[K, V] {
	idx := (reverse >> (4 * 15))

	return &h.buckets[idx]
}

func (h *Map[K, V]) searchBucket4update(k uint64) (b *bucket[K, V], r uint64) {

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

func (h *Map[K, V]) searchBucket4Key4(k uint64, ignoreBucketEnry bool) (b *bucket[K, V], reverse uint64, ignore bool) {
	b, reverse = h.searchBucket4update(k)
	if h.modeForBucket != CombineSearch {
		if p := b.prevAsB(); nearUint64(p.reverse, b.reverse, reverse) != b.reverse {
			b = p
		}
	}
	ignore = ignoreBucketEnry
	return
}

func nearBucketFromCache[K Key[K], V any](levels [16]*bucket[K, V], lbNext *bucket[K, V], reverseNoMask uint64) (result *bucket[K, V]) {
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

func (h *Map[K, V]) searchBybucket(lbCur *bucket[K, V], reverseNoMask uint64, ignoreBucketEnry bool) *embeddedEntry[K, V] {
	if h.isEmbededItemInBucket {
		Log(LogWarn, "should use bsearchBybucket() not searchBybucket() in embedded bucket")
		return h.bsearchBybucket(lbCur, reverseNoMask, ignoreBucketEnry)
	}

	return h._searchBybucket(lbCur, reverseNoMask, ignoreBucketEnry)
}

func (h *Map[K, V]) _searchBybucket(lbCur *bucket[K, V], reverse uint64, ignore bool) *embeddedEntry[K, V] {
	return h.searchCopyBucket[noDummyTrace](lbCur, reverse, ignore, nil)
}

func (h *Map[K, V]) searchCopyBucket[T dummyTrace](lbCur *bucket[K, V], reverse uint64, ignore bool, dummy **MapHead) *embeddedEntry[K, V] {
	if lbCur == nil {
		return nil
	}
	b := lbCur
	if h.modeForBucket == CombineSearch2 && b.reverse > reverse {
		b = b.NextOnLevel()
	}
	return h.searchCopyEntry[T](b, b.entry(h), reverse, ignore, dummy)
}

// Delete marks the matching entry deleted so subsequent lookups cannot find it.
//
// Delete returns false when it does not find key, or when another Delete or
// Purge of key deleted its item first. Without UseEmbeddedPool, it returns
// false also, doing nothing, when the item left the map after Delete found
// it, and while another call is working on the item of key: a StoreItem or a
// Set of the item, which Get may find before it returns, or a StoreItem of
// the item that returns false as the item is linked already.
func (h *Map[K, V]) Delete(key K) bool {
	_, _, mh, ok := h.deleteItem(key)
	if mh != nil {
		mh.releaseBusy()
	}
	return ok
}

// deleteItem marks the item of key deleted, and returns it and the bucket
// that the lookup found it in, when this call marked it. Without
// UseEmbeddedPool it returns also the node whose mapIsBusy it holds, for the
// caller to release.
func (h *Map[K, V]) deleteItem(key K) (*embeddedEntry[K, V], *bucket[K, V], *MapHead, bool) {

	k, conflict := key.KeyHash()
	var item *embeddedEntry[K, V]
	var bucket *bucket[K, V]
	var lock *trylock.Mutex
	var dummy *MapHead
	for {
		var ok bool
		item, bucket, dummy, ok = h.lookupItem[saveDummyTrace](k, conflict, key, true)
		if !ok {
			return nil, nil, nil, false
		}
		if stepEnabled {
			stepAt("delete.found", unsafe.Pointer(item.PtrListHead()), nil)
		}
		if !h.isEmbededItemInBucket {
			break
		}
		lock, ok = h.lockFoundItem(key, item, bucket)
		if ok {
			break
		}
		runtime.Gosched()
	}
	if lock != nil {
		defer lock.Unlock()
	}
	// the item pool moves the item to a larger array: a delete that found
	// the item and one that found its copy claim one node
	if entry := item; !h.isEmbededItemInBucket {
		return h.deleteEntry(entry, bucket, dummy)
	}
	origin := elist_head.FindOrigin(item.PtrListHead())
	mh := mapheadFromLListHead(origin)
	var hold mapState
	if !h.isEmbededItemInBucket {
		hold = mapIsBusy
	}
	won, busy := mh.claimDelete(hold)
	stepAt("delete.claimed", unsafe.Pointer(item.PtrListHead()), nil)
	if busy {
		return nil, nil, nil, false
	}
	if won && hold != 0 && item.PtrListHead().IsSingle() {
		// item left the list before this call claimed it
		atomic.AndUint64((*uint64)(&mh.state), ^uint64(mapIsDeleted|mapIsBusy))
		return nil, nil, nil, false
	}
	if !won && origin == item.PtrListHead() && !elist_head.IsMoved(item.PtrListHead()) {
		// another delete of the key got there first, and item has no copies
		// to delete: a StoreItem may have linked item again since
		return nil, nil, nil, false
	}
	if won {
		// a delete that finds another node of the line of the item once
		// origin is gone claims that node
		elist_head.EachOfLine(item.PtrListHead(), func(n *elist_head.ListHead) {
			atomic.OrUint64((*uint64)(&mapheadFromLListHead(n).state), uint64(mapIsDeleted))
		})
	}
	item.Delete()
	// the item pool may have copied the item before it was deleted: the
	// copy is deleted too, also by a delete that another delete of the key
	// got ahead of on the origin, before it returns
	for head := item.PtrListHead(); elist_head.IsMoved(head); {
		if head = elist_head.MovedTo(head); head == nil {
			break
		}
		atomic.OrUint64((*uint64)(&mapheadFromLListHead(head).state), uint64(mapIsDeleted))
	}
	// a delete that finds a node of the line meanwhile claims origin as long
	// as origin is kept, and that node after it is deleted
	runtime.KeepAlive(origin)
	if !won {
		// another delete of the key got there first
		return nil, nil, nil, false
	}
	h.AddLen(-1)
	if hold == 0 {
		return item, bucket, nil, true
	}
	return item, bucket, mh, true
}

// Purge ... key/value entry from map.
//
// Without UseEmbeddedPool, Purge returns false where Delete does, and then
// does not take the item out of the list.
func (h *Map[K, V]) Purge(key K) bool {
	if h.isEmbededItemInBucket {
		return h.purgeInEmbedded(key)
	}
	return h.purgeItem(key)
}

// purgeItem deletes the item of key as Delete does, and then takes it out of
// the list of entries, in the order of purgeInEmbedded.
func (h *Map[K, V]) purgeItem(key K) bool {
	item, bucket, mh, ok := h.deleteItem(key)
	if !ok {
		return false
	}
	defer mh.releaseBusy()
	head := item.PtrListHead()
	if !mh.isPoolItem() {
		// A split may have changed the bucket since the first lookup.
		k, _ := item.KeyHash()
		bucket, _ = h.searchBucket4update(k)
	}
	for {
		// an item that the item pool moves stays linked, with its marks;
		// so do the links that a Set of the item wrote meanwhile
		err := head.MarkForDelete()
		if err != elist_head.ErrMoved {
			head.InitMarked()
		}
		// the entry left the list: the bucket counts one entry less, as
		// setItem counted it, and is not split for entries that are gone
		if err == nil && bucket.level() > 0 {
			atomic.AddInt32(&bucket._len, -1)
		}
		// the item pool moves or moved the item to a larger array: the
		// copy is deleted too
		c := elist_head.MovedTo(head)
		if c == nil {
			break
		}
		head = c
		atomic.OrUint64((*uint64)(&mapheadFromLListHead(head).state), uint64(mapIsDeleted))
	}
	return true
}

func (h *Map[K, V]) purgeInEmbedded(key K) bool {
	var item *embeddedEntry[K, V]
	var bucket *bucket[K, V]
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
	if !item.isPoolItem() {
		return true
	}

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
// It panics with UseEmbeddedPool; use Range to visit keys and values instead.
// RangeItem does not provide a snapshot during concurrent updates.
func (h *Map[K, V]) RangeItem(f func(*Entry[K, V]) bool) {
	h.checkEntryAccess()
	h.rangeItem(func(e *embeddedEntry[K, V]) bool { return f(e.viewEntry()) })
}

func (h *Map[K, V]) rangeItem(f func(*embeddedEntry[K, V]) bool) {
	defer runtime.KeepAlive(h)
	for cur := h.head.DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
		mh := mapheadFromLListHead(cur)
		if mh.IsIgnored() {
			continue
		}
		e := entryHMapFromListHead[K, V](cur)
		if !h.isEmbededItemInBucket {
			h.rangeCopyEntries(e, f)
			return
		}
		stepAt("range.item", unsafe.Pointer(e.PtrListHead()), nil)
		if !f(e) {
			return
		}
	}
}

// Range ... calls f sequentially for each key and value present in the map.
// order is reverse key order
// Range does not provide a snapshot during concurrent updates.
func (h *Map[K, V]) Range(f func(K, V) bool) {
	h.rangeItem(func(e *embeddedEntry[K, V]) bool {
		key, value, state := e.loadTypedKeyValue()
		if state&(mapIsDummy|mapIsDeleted) != 0 {
			return true
		}
		if stepEnabled {
			stepAt("range.keyRead", unsafe.Pointer(e.PtrListHead()), nil)
		}
		return f(key, value)
	})
}

// First returns the first live value in Range order. It returns false when
// empty or when concurrent removal or reuse invalidates the candidate.
func (h *Map[K, V]) First() (V, bool) { return h.endValue(false) }

// Last returns the last live value in Range order. It returns false when
// empty or when concurrent removal or reuse invalidates the candidate.
func (h *Map[K, V]) Last() (V, bool) { return h.endValue(true) }

func (h *Map[K, V]) first() *embeddedEntry[K, V] {
	for head := h.head.DirectNext(); head != h.tail; head = head.DirectNext() {
		if head.Empty() {
			return nil
		}
		if !mapheadFromLListHead(head).IsIgnored() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}

func (h *Map[K, V]) last() *embeddedEntry[K, V] {
	for head := h.tail.DirectPrev(); head != h.head; head = head.DirectPrev() {
		if head.Empty() {
			return nil
		}
		if !mapheadFromLListHead(head).IsIgnored() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}

func (h *Map[K, V]) allpools() (pools []*samepleItemPool[K, V]) {
	if h.pooler == nil {
		return
	}

	for i := range h.pooler.itemPool {
		pools = append(pools, samepleItemPoolFromListHead[K, V](h.pooler.itemPool[i].Next()))
	}
	return
}

func (h *Map[K, V]) FindBucket(reverse uint64) (b *bucket[K, V]) {
	return h._findBucket(reverse, false, false)
}

func (h *Map[K, V]) findBucket(reverse uint64) (b *bucket[K, V]) {

	return h._findBucket(reverse, false, false)
}

func (h *Map[K, V]) _findBucket(reverse uint64, ignoreNoPool bool, ignoreNoInitDummy bool) (b *bucket[K, V]) {

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
		var bucketDowns *bucketSlice[K, V]
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

func (h *Map[K, V]) bucketFromPool(reverse uint64, opts ...cOptFn) (b *bucket[K, V], onOk func()) {
	// h.mu.Lock()
	// defer h.mu.Unlock()

	opt := &commonOpt{}
	prevs := opt.Option(opts...)
	defer opt.Option(prevs...)

	level := int32(0)
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	bucketNotInits := []*bucket[K, V]{}

	for l := int32(1); l <= level; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
	RETRY_INITIALIZE:
		if atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, 0, 1) {

			downLevels := make([]bucket[K, V], 1, 16)

			downLevels[0].setLevel(b.childLevel())
			downLevels[0].reverse = b.reverse
			downLevels[0].Init()
			downLevels[0].LevelHead.Init()
			downLevels[0]._parent = b
			downLevels[0].setItemPoolFn = func(p *samepleItemPool[K, V]) {
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
		active := atomic.LoadInt32(&b.cntOfActiveLevels)
		if active <= int32(idx) && active == int32(b.ptrDownLevels().Len()) && atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, active, int32(idx)+1) {
			// The successful CAS claimed idx. Another splitter may already have
			// advanced the shared count before this goroutine initializes its slot.
			cidx := idx
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
			finish := func() {
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
			nDownLevel.onOkFn.Store(&finish)
			onOk = finish

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
				if finish := last.onOkFn.Load(); finish != nil {
					(*finish)()
				}
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
			finish := onOk
			down.onOkFn.Store(&finish)

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
		if finish := b.onOkFn.Load(); finish != nil {
			onOk = *finish
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

func (h *Map[K, V]) setupBcukets(buckets []*bucket[K, V]) {

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
