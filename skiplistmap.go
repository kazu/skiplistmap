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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
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
}

type LevelHead list_head.ListHead

// 	eBuf       []entryBuffer
// 	maxPefEbuf int
// }

//type LevelHead list_head.ListHead

type mapState byte

const (
	mapIsDummy mapState = 1 << iota
	mapIsDeleted
)

type MapHead struct {
	state    mapState
	conflict uint64
	reverse  uint64
	elist_head.ListHead
}

var EmptyMapHead *MapHead = (*MapHead)(unsafe.Pointer(uintptr(0)))

func (mh *MapHead) KeyInHmap() uint64 {
	return bits.Reverse64(mh.reverse)
}

func (mh *MapHead) IsIgnored() bool {
	return mh.state > 0
}

func (mh *MapHead) IsDummy() bool {
	return mh.state&mapIsDummy > 0
}

func (mh *MapHead) IsDeleted() bool {
	return mh.state&mapIsDeleted > 0
}

func (mh *MapHead) ConflictInHamp() uint64 {
	return mh.conflict
}

func (mh *MapHead) PtrListHead() *elist_head.ListHead {
	return &(mh.ListHead)
}

const mapheadOffset = unsafe.Offsetof(EmptyMapHead.ListHead)

func (mh *MapHead) Offset() uintptr {
	return mapheadOffset
}

func (mh *MapHead) fromListHead(l *elist_head.ListHead) *MapHead {
	return (*MapHead)(ElementOf(unsafe.Pointer(l), mapheadOffset))
}

func (c *MapHead) FromListHead(l *elist_head.ListHead) elist_head.List {
	return c.fromListHead(l)
}

func (c *MapHead) NextWithNil() *MapHead {
	if c.Next() == &c.ListHead {
		return nil
	}
	return c.fromListHead(c.Next())
}

func (c *MapHead) PrevtWithNil() *MapHead {
	if c.Prev() == &c.ListHead {
		return nil
	}
	return c.fromListHead(c.Prev())
}

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
		if !enable {
			h.pooler = nil
		} else if h.pooler == nil {
			h.pooler = newPool()
			h.pooler.startMgr()
		}

		return UsePool(!enable)
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
	list_head.MODE_CONCURRENT = true
	hmap := &Map{len: 0, maxPerBucket: 32}

	topBucket := &bucket{}
	topBucket.InitAsEmpty()
	hmap.tailBucket = topBucket.Next()
	hmap.headBucket = topBucket.Prev()

	hmap.head = &elist_head.ListHead{}
	hmap.tail = &elist_head.ListHead{}
	elist_head.InitAsEmpty(hmap.head, hmap.tail)

	hmap.modeForBucket = NestedSearchForBucket
	hmap.ItemFn = func() MapItem { return emptyEntryHMap }

	hmap.Options(opts...)

	// other traverse option is not required if not marking delete.
	list_head.DefaultModeTraverse.Option(list_head.Direct())

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

	btable := &bucket{
		level: 16,
		len:   0,
	}
	btable.reverse = ^uint64(0)
	btable.Init()
	btable.LevelHead.Init()

	empty := &btable.dummy
	empty.key, empty.value = nil, nil
	empty.reverse, empty.conflict = btable.reverse, 0
	empty.PtrMapHead().state |= mapIsDummy

	h.add2(h.head.Prev().Next(), empty)

	// add bucket
	h.tailBucket.InsertBefore(&btable.ListHead)
	btable.head = &empty.ListHead

	levelBucket := h.levelBucket(btable.level)
	levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
	h.setLevel(btable.level, levelBucket)
	btablefirst := btable

	topReverses := make([]uint64, 16)

	for k := uint64(0); k < 16; k++ {
		topReverses[int(k)] = bits.Reverse64(k)
	}
	sort.Slice(topReverses, func(i, j int) bool { return topReverses[i] < topReverses[j] })

	for i := range topReverses {
		reverse := topReverses[i]
		btable = &h.buckets[i]
		btable.level, btable.len, btable.reverse = 1, 0, reverse
		btable.Init()
		btable.LevelHead.Init()

		empty = &btable.dummy
		empty.key, empty.value = nil, nil
		empty.reverse, empty.conflict = btable.reverse, 0
		empty.PtrMapHead().state |= mapIsDummy

		btablefirst.head.InsertBefore(&empty.ListHead)

		// add bucket
		btablefirst.Next().InsertBefore(&btable.ListHead)
		if IsDebug() {
			h.validateBucket(btable)
		}

		btable.head = &empty.ListHead
		btable.LevelHead.Init()
		if i > 0 {
			h.buckets[i-1].LevelHead.InsertBefore(&btable.LevelHead)
		} else {
			levelBucket = h.levelBucket(btable.level)
			levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
		}

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

func (h *Map) _set(k, conflict uint64, btable *bucket, item MapItem) bool {

	item.PtrMapHead().reverse = bits.Reverse64(k)
	item.PtrMapHead().conflict = conflict

	h.initBeforeSet()

	var addOpt HMethodOpt
	_ = addOpt

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

	if btable != nil && btable.head == nil {
		Log(LogWarn, "bucket.head not set")
	}
	if btable == nil || btable.head == nil {
		btable = &bucket{}
		btable.head = h.head.Prev().Next()
	} else {
		addOpt = WithBucket(btable)
	}

	entry, cnt := h.find(btable.head, func(item HMapEntry) bool {
		mHead := item.PtrMapHead()
		return bits.Reverse64(k) <= mHead.reverse
	}, ignoreBucketEntry(false))
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
		tStart = btable.head
	}

	item.PtrListHead().Init()
	if addOpt == nil {
		h.add2(tStart, item)
	} else {
		h.add2(tStart, item, addOpt)
	}
	atomic.AddInt64(&h.len, 1)
	if btable.level > 0 {
		atomic.AddInt32(&btable.len, 1)
	}
	return true
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
	item, _, ok := h._get2(k, conflict)
	return item, ok
}

func (h *Map) _get2(k, conflict uint64) (MapItem, *bucket, bool) {

	if EnableStats {
		h.mu.Lock()
		DebugStats[CntOfGet]++
		h.mu.Unlock()
	}
	var bucket *bucket
	var reverse uint64
	switch h.modeForBucket {
	case CombineSearch, CombineSearch2, CombineSearch3, CombineSearch4:

		//e := h.searchKey(k, true)
		bucket, reverse = h.searchBucket4update(k)
		e := h.searchBybucket(bucket, reverse, true)

		if e == nil {
			if Failreverse == 0 {
				Failreverse = bits.Reverse64(k)
			}
			return nil, bucket, false
		}
		if e.PtrMapHead().reverse != bits.Reverse64(k) || e.PtrMapHead().conflict != conflict {
			return nil, bucket, false
		}
		return e.(MapItem), bucket, true

	default:
		bucket = h.searchBucket(k)
		break
	}

	if bucket == nil {
		return nil, nil, false
	}
	var e *entryHMap

	if h.modeForBucket == FalsesSearchForBucket {
		return nil, bucket, true
	}

	if e == nil {
		return nil, bucket, false
	}
	if e.reverse != bits.Reverse64(k) || e.conflict != conflict {
		return nil, bucket, false
	}

	return e, bucket, true

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

// GetWithFn ... Get with succes function.
func (h *Map) GetWithFn(key interface{}, onSuccess func(interface{})) bool {

	item, ok := h._get(KeyToHash(key))
	if !ok {
		return false
	}
	onSuccess(item.Value())
	return ok
}

// LoadItem ... return key/value item with embedded-linked-list. if not found, ok is false
func (h *Map) LoadItem(key interface{}) (item MapItem, success bool) {
	item, _, success = h.loadItem(0, 0, key)
	return
}

func (h *Map) LoadItemByHash(k uint64, conflict uint64) (item MapItem, success bool) {

	item, _, success = h.loadItem(k, conflict, nil)
	return
}

func (h *Map) loadItem(k uint64, conflict uint64, key interface{}) (MapItem, *bucket, bool) {
	//return h._get(k, conflict)
	if key == nil {
		return h._get2(k, conflict)
	}

	return h._get2(KeyToHash(key))
}

// Set ... set the value for a key
func (h *Map) Set(key, value interface{}) bool {

	item, bucket, found := h.loadItem(0, 0, key)
	if found {
		return h._update(item, value)
	}

	var s *SampleItem
	useDump := false

	if h.pooler == nil && h.modeForBucket == CombineSearch4 {
		UsePool(true)(h)
	}

	if h.pooler != nil {
		k, _ := KeyToHash(key)
		var wg sync.WaitGroup
		wg.Add(1)
		h.pooler.Get(bits.Reverse64(k), func(item MapItem) {
			s = item.(*SampleItem)
			wg.Done()
		})
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
	} else {
		s = &SampleItem{}
	}

	s.K, s.V = key.(string), value

	if _, ok := h.ItemFn().(*SampleItem); !ok {
		ItemFn(func() MapItem {
			return EmptySampleHMapEntry
		})(h)
	}
	k, conflict := KeyToHash(s.K)
	return h._set(k, conflict, bucket, s)
}

// StoreItem ... set key/value item with embedded-linked-list
func (h *Map) StoreItem(item MapItem) bool {
	k, conflict := item.KeyHash()

	oitem, bucket, found := h.loadItem(k, conflict, nil)
	if found {
		return h._update(oitem, item.Value())
	}

	return h._set(k, conflict, bucket, item)
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
func (h *Map) find(start *elist_head.ListHead, cond func(HMapEntry) bool, opts ...searchArg) (result HMapEntry, cnt int) {

	conf := sharedSearchOpt
	previous := conf.Options(opts...)
	defer func() {
		if previous != nil {
			conf.Options(previous)
		}
	}()
	cnt = 0
	var e MapItem
	if start.Empty() {
		return
	}
	for cur := start; cur != cur.Next(); cur = cur.Next() {
		e = entryHMapFromListHead(cur)

		if conf.ignoreBucketEntry && e.PtrMapHead().IsIgnored() {
			continue
		}
		if cond(e) {
			result = e
			return
		}
		cnt++
	}
	return nil, cnt

}

func (h *Map) MakeBucket(ocur *elist_head.ListHead, back int) (err error) {

	enableDumpBucket := false

	cur := ocur
	cur = cur.Prev()

	e := entryHMapFromListHead(cur)
	cBucket := h.searchBucket(bits.Reverse64(e.reverse))
	if cBucket == nil || cBucket.reverse > e.reverse {
		return ErrNotFoundBUcket
	}
	nextBucket := cBucket
	for ; nextBucket.prevAsB() != nextBucket; nextBucket = nextBucket.prevAsB() {

		if nextBucket.reverse > e.reverse {
			break
		}
		if nextBucket.reverse <= e.reverse && nextBucket.reverse > cBucket.reverse {
			cBucket = nextBucket
		}
	}
	if nextBucket.reverse < e.reverse || cBucket.reverse > e.reverse {
		return ErrNotFoundBUcket
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

	b := h.bucketFromPool(newReverse)

	if b == nil {
		if IsDebug() {
			h.searchBucket(bits.Reverse64(e.reverse))
		}

		return ErrFailBucketAlloc
	}
	//b.reverse, b.level, b.len = newReverse, level, 0
	if b.reverse != newReverse {
		b.reverse, b.len = newReverse, 0
	}

	if b.reverse == 0 && b.level > 1 {
		err = NewError(EInvalidBucket, "bucket.reverse = 0. but level 1= 1", nil)
		Log(LogWarn, err.Error())
		return
	}

	b.Init()
	b.LevelHead.Init()

	for cur := cBucket.head.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		b.len++
		e := entryHMapFromListHead(cur)
		if e.reverse > b.reverse {
			break
		}
	}

	atomic.AddInt32(&cBucket.len, -b.len)
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

		var nNext, nPrev *bucket
		if !b.LevelHead.DirectPrev().Empty() {
			nPrev = bucketFromLevelHead(b.LevelHead.Prev())
		}
		if !b.LevelHead.DirectNext().Empty() {
			nNext = bucketFromLevelHead(b.LevelHead.Next())
		}
		_, _ = nNext, nPrev

	} else {
		Log(LogWarn, "not found level bucket.")
	}
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

	if int(b.len) > h.maxPerBucket {
		h.MakeBucket(cBucket.head.Next(), int(b.len)/2)
	}
	if int(cBucket.len) > h.maxPerBucket {
		h.MakeBucket(nextBucket.head.Prev(), int(b.len)/2)
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

func (h *Map) add2(start *elist_head.ListHead, e HMapEntry, opts ...HMethodOpt) bool {
	var opt *hmapMethod
	if len(opts) > 0 {
		opt = &hmapMethod{}
		for _, fn := range opts {
			fn(opt)
		}
	}

	cnt := 0
	pos, _ := h.find(start, func(ehead HMapEntry) bool {
		cnt++
		return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
	}, ignoreBucketEntry(false))

	defer func() {
		if !EnableStats || e.PtrMapHead().IsIgnored() {
			return
		}

		if h.SearchKey(bits.Reverse64(e.PtrMapHead().reverse), ignoreBucketEntry(false)) == nil {
			sharedSearchOpt.Lock()
			sharedSearchOpt.e = ErrInvalidAdd
			sharedSearchOpt.Unlock()
		}
	}()

	if pos != nil {
		pos.PtrListHead().InsertBefore(e.PtrListHead())
		if opt == nil || opt.bucket == nil {
			return true
		}
		btable := opt.bucket
		if btable != nil && !e.PtrMapHead().IsIgnored() && int(btable.len) > h.maxPerBucket {
			h.MakeBucket(e.PtrListHead(), int(btable.len)/2)
		}
		return true
	}
	if opt != nil && opt.bucket != nil && opt.bucket.entry(h) != nil {
		// pos, _ = h.find(start, func(ehead HMapEntry) bool {
		// 	return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
		// }, ignoreBucketEntry(false))
		nextE := nextAsE(opt.bucket.entry(h))

		nextE.PtrListHead().InsertBefore(e.PtrListHead())
		return true
	}
	h.tail.InsertBefore(e.PtrListHead())
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
			btable.reverse, btable.len, btable.head, btable.level, &btable.LevelHead, btable.LevelHead.DirectPrev(), btable.LevelHead.DirectNext(), len(btable.downLevels))
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
				cBucket.reverse, cBucket.len, cBucket.head, cBucket.level, &cBucket.LevelHead, cBucket.LevelHead.DirectPrev(), cBucket.LevelHead.DirectNext(), len(cBucket.downLevels))

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
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
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

func (mhead *MapHead) dump(w io.Writer) {

	e := fromMapHead(mhead)

	var ekey interface{}
	ekey = e.Key()
	fmt.Fprintf(w, "  entryHMap{key: %+10v, k: 0x%16x, reverse: 0x%16x), conflict: 0x%x, cur: %p, prev: %p, next: %p}\n",
		ekey, bits.Reverse64(mhead.reverse), mhead.reverse, mhead.conflict, mhead.PtrListHead(), mhead.PtrListHead().DirectPrev(), mhead.PtrListHead().DirectNext())

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
		thead = tBucket.head.Prev().Next()
	}
	h.add2(thead, empty)
	nBtable.head = &nBtable.dummy.ListHead

	tBucket := bucketFromListHead(tBtable)
	if IsDebug() {
		h.validateBucket(tBucket)
	}

	// add bucket
	tBtable.InsertBefore(&nBtable.ListHead)

	if IsDebug() {
		h.validateBucket((tBucket))
		h.validateBucket((nBtable))
	}

	nBtable.head = &empty.ListHead
}

func (h *Map) addBucket(nBtable *bucket) {

	for bcur := h.headBucket.Prev().Next(); !bcur.Empty(); bcur = bcur.Next() {
		cBtable := bucketFromListHead(bcur)
		if cBtable.reverse == nBtable.reverse {
			return
		}

		if cBtable.reverse < nBtable.reverse {
			h._InsertBefore(&cBtable.ListHead, nBtable)
			if nBtable.reverse <= cBtable.reverse {
				Log(LogError, "brokne relation bucket")
			}
			return
		}
	}

}

func (h *Map) findNextLevelBucket(reverse uint64, level int32) (cur *list_head.ListHead) {

	bcur := h.levelBucket(level)
	if bcur == nil {
		return nil
	}
	prevs := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	front := bcur.LevelHead.Front()
	list_head.DefaultModeTraverse.Option(prevs...)
	bcur = bucketFromLevelHead(front.DirectPrev().DirectNext())

	cnt := 0
	for cur := bcur; cur != cur.NextOnLevel(); cur = cur.NextOnLevel() {
		cnt++
		if cnt > 1000 {
			return &cur.LevelHead
		}
		if reverse > cur.reverse {
			return &cur.LevelHead
		}

	}
	if bcur.Empty() {
		return &bcur.LevelHead
	}
	if bcur.NextOnLevel() == bcur {
		return &bcur.LevelHead
	}
	nCur := bcur.NextOnLevel()
	return &nCur.LevelHead
}

func (h *Map) initLevels() {

	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.levels {
		b := &bucket{level: int32(i) + 1}
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

func fromMapHead(mhead *MapHead) MapItem {

	if mhead.IsDummy() {
		return entryHMapFromListHead(mhead.PtrListHead())
	}
	return SampleItemFromListHead(mhead.PtrListHead())
}

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

type searchOpt struct {
	h                 *Map
	e                 error
	ignoreBucketEntry bool
	sync.Mutex
}

var sharedSearchOpt *searchOpt = &searchOpt{ignoreBucketEntry: true}

type searchArg func(*searchOpt) searchArg

func ignoreBucketEntry(t bool) searchArg {

	return func(opt *searchOpt) searchArg {
		prev := opt.ignoreBucketEntry
		opt.ignoreBucketEntry = t
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
func (h *Map) SearchKey(k uint64, opts ...searchArg) HMapEntry {

	conf := sharedSearchOpt
	previous := conf.Options(opts...)
	defer func() {
		if previous != nil {
			conf.Options(previous)
		}
	}()
	return h.searchKey(k, conf.ignoreBucketEntry)

}

func (h *Map) searchKey(k uint64, ignoreBucketEnry bool) HMapEntry {
	return h.searchBybucket(h.searchBucket4Key4(k, ignoreBucketEnry))
	//return h.searchBybucket(h.searchBucket4Key3(k, ignoreBucketEnry))
}

func (h *Map) topLevelBucket(reverse uint64) *bucket {
	idx := (reverse >> (4 * 15))

	return &h.buckets[idx]
}

func (h *Map) searchBucket4update(k uint64) (*bucket, uint64) {

	reverseNoMask := bits.Reverse64(k)

	cBuf := h.findBucket(reverseNoMask)

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
		if int32(i)+1 == result.level {
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
	if lbCur == nil {
		return nil
	}

	lbNext := lbCur

	if h.modeForBucket == CombineSearch2 && lbCur.reverse > reverseNoMask {
		lbNext = lbCur.NextOnLevel()
	}

	if lbNext.reverse < reverseNoMask {
		result := lbNext.entry(h)
		for cur := result.PtrMapHead(); cur != nil; cur = cur.NextWithNil() {
			if EnableStats && ignoreBucketEnry {
				h.mu.Lock()
				DebugStats[CntReverseSearch]++
				h.mu.Unlock()
			}
			if ignoreBucketEnry && cur.IsIgnored() {
				continue
			}
			curReverse := cur.reverse
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

//Delete ... set nil to the key of MapItem. cannot Get entry
func (h *Map) Delete(key interface{}) {

	item, ok := h.LoadItem(key)
	if !ok {
		return
	}
	item.Delete()
}

// RangeItem ... calls f sequentially for each key and value present in the map.
// called ordre is reverse key order
func (h *Map) RangeItem(f func(MapItem) bool) {

	oldConfs := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	defer list_head.DefaultModeTraverse.Option(oldConfs...)

	for cur := h.head.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
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

func (h *Map) First() HMapEntry {
	cur := h.head.DirectPrev().DirectNext()
	return h.ItemFn().HmapEntryFromListHead(cur)
}

func (h *Map) Last() HMapEntry {
	cur := h.tail.DirectNext().DirectPrev()
	return h.ItemFn().HmapEntryFromListHead(cur)
}
