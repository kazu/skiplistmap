// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

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

	NoItemSearchForBucket = 9 // test mode
	FalsesSearchForBucket = 10
)

// Map ... Skip List Map is an ordered and concurrent map.
// this Map is gourtine safety for reading/updating/deleting, require locking and coordination. This
type Map struct {
	buckets    bucket
	lastBucket *list_head.ListHead

	buf *bucketBuffer

	len          int64
	maxPerBucket int
	start        *list_head.ListHead
	last         *list_head.ListHead

	modeForBucket SearchMode
	mu            sync.Mutex
	levels        [16]atomic.Value

	ItemFn func() MapItem
}

type LevelHead list_head.ListHead

type entryHMap struct {
	key   interface{}
	value interface{}

	MapHead
}

func NewEntryMap(key, value interface{}) *entryHMap {
	return &entryHMap{
		key:   key,
		value: value,
	}
}

var (
	emptyEntryHMap *entryHMap = &entryHMap{}
	emptyBucket    *bucket    = &bucket{}
	EmptyEntryHMap *entryHMap = emptyEntryHMap
)

func entryHMapFromListHead(head *list_head.ListHead) *entryHMap {
	return (*entryHMap)(list_head.ElementOf(emptyEntryHMap, head))
}

func (e *entryHMap) entryHMapromListHead(lhead *list_head.ListHead) *entryHMap {
	return entryHMapFromListHead(lhead)
}

func (s *entryHMap) HmapEntryFromListHead(lhead *list_head.ListHead) HMapEntry {
	return s.entryHMapromListHead(lhead)
}

func (s *entryHMap) Key() interface{} {
	return s.key
}

func (s *entryHMap) Value() interface{} {
	return s.value
}

func (s *entryHMap) SetValue(v interface{}) bool {
	s.value = v
	return true
}

func (s *entryHMap) Next() HMapEntry {
	return s.entryHMapromListHead(s.PtrListHead().DirectNext())
}
func (s *entryHMap) Prev() HMapEntry {
	return s.entryHMapromListHead(s.PtrListHead().DirectPrev())
}

func (s *entryHMap) PtrMapHead() *MapHead {
	return &s.MapHead
}
func (s *entryHMap) Offset() uintptr {
	return unsafe.Offsetof(s.ListHead)
}

func (s *entryHMap) Delete() {
	s.key = nil
	s.MapHead.isDummy = true
}

type HMapEntry interface {
	Offset() uintptr
	PtrMapHead() *MapHead
	PtrListHead() *list_head.ListHead
	HmapEntryFromListHead(*list_head.ListHead) HMapEntry
	Next() HMapEntry
	Prev() HMapEntry
}
type MapItem interface {
	Key() interface{}   // require order for HMap
	Value() interface{} // require order for HMap
	SetValue(interface{}) bool
	Delete()

	HMapEntry
}

type MapHead struct {
	//k        uint64
	conflict uint64
	reverse  uint64
	isDummy  bool
	list_head.ListHead
}

var EmptyMapHead MapHead = MapHead{}

func (mh *MapHead) KeyInHmap() uint64 {
	return bits.Reverse64(mh.reverse)
}

func (mh *MapHead) ConflictInHamp() uint64 {
	return mh.conflict
}

func (mh *MapHead) PtrListHead() *list_head.ListHead {
	return &(mh.ListHead)
}

func (mh *MapHead) Offset() uintptr {
	return unsafe.Offsetof(mh.ListHead)
}

func (mh *MapHead) fromListHead(l *list_head.ListHead) *MapHead {
	return (*MapHead)(list_head.ElementOf(&EmptyMapHead, l))
}

func (c *MapHead) FromListHead(l *list_head.ListHead) list_head.List {
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
	hmap.buckets.InitAsEmpty()
	hmap.buckets = *(bucketFromListHead(hmap.buckets.Prev()))
	hmap.lastBucket = hmap.buckets.Next()
	list := &list_head.ListHead{}
	list.InitAsEmpty()
	hmap.start = list.Prev()
	hmap.last = list.Next()
	hmap.modeForBucket = NestedSearchForBucket
	hmap.ItemFn = func() MapItem { return emptyEntryHMap }

	hmap.Options(opts...)

	// other traverse option is not required if not marking delete.
	list_head.DefaultModeTraverse.Option(list_head.Direct())

	hmap.initLevels()

	hmap.buf = newBucketBuffer(1)

	return hmap
}

// func (h *HMap) set(key, value interface{}) bool {
// 	k, conflict := KeyToHash(key)
// 	return h._set(k, conflict, key, value)
// }

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

	empty := &entryHMap{
		key:   nil,
		value: nil,
	}
	empty.reverse, empty.conflict = btable.reverse, 0
	empty.PtrMapHead().isDummy = true

	empty.Init()

	h.add2(h.start.Prev().Next(), empty)

	h.buckets.Prev().Next().InsertBefore(&btable.ListHead)
	btable.start = &empty.ListHead

	levelBucket := h.levelBucket(btable.level)
	levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
	h.setLevel(btable.level, levelBucket)
	btablefirst := btable

	topReverses := make([]uint64, 16)

	for k := uint64(0); k < 16; k++ {
		topReverses[int(k)] = bits.Reverse64(k)
	}
	sort.Slice(topReverses, func(i, j int) bool { return topReverses[i] < topReverses[j] })

	levelBuf := h.buf.fromLevel(1)
	for i := range topReverses {
		reverse := topReverses[i]
		btable = &levelBuf.buf[i]
		btable.level, btable.len, btable.reverse = 1, 0, reverse
		btable.Init()
		btable.LevelHead.Init()

		empty = &entryHMap{
			key:   nil,
			value: nil,
		}
		empty.reverse, empty.conflict = btable.reverse, 0
		empty.PtrMapHead().isDummy = true

		empty.Init()
		btablefirst.start.InsertBefore(&empty.ListHead)

		btablefirst.Next().InsertBefore(&btable.ListHead)
		btable.start = &empty.ListHead
		btable.LevelHead.Init()
		if i > 0 {
			h.buf.fromLevel(1).buf[i-1].LevelHead.InsertBefore(&btable.LevelHead)
			//h.topBuckets[i-1].LevelHead.InsertBefore(&btable.LevelHead)
		} else {
			levelBucket = h.levelBucket(btable.level)
			levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
		}

		//h.topBuckets[i] = btable
	}

	if EnableStats {
		fmt.Printf("%s\n", h.DumpBucket())
		fmt.Printf("%s\n", h.DumpEntry())
	}
}

//FIXME: renate _set
func (h *Map) _set(k, conflict uint64, item MapItem) bool {

	item.PtrMapHead().reverse = bits.Reverse64(k)
	item.PtrMapHead().conflict = conflict

	h.initBeforeSet()

	var btable *bucket
	var addOpt HMethodOpt
	_ = addOpt

	if h.modeForBucket != CombineSearch && h.modeForBucket != CombineSearch3 {
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
			//fmt.Printf("%s\n", h.DumpBucketPerLevel())
			p := btable.PrevOnLevel()
			_ = p
			btable = h.searchBucket(k)
		}

	}

	if btable != nil && btable.start == nil {
		Log(LogWarn, "bucket.start not set")
	}
	if btable == nil || btable.start == nil {
		btable = &bucket{}
		btable.start = h.start.Prev().Next()
	} else {
		addOpt = WithBucket(btable)
	}

	entry, cnt := h.find(btable.start, func(item HMapEntry) bool {
		return bits.Reverse64(k) <= item.PtrMapHead().reverse
	}, ignoreBucketEntry(false))
	_ = cnt
	if entry != nil && entry.PtrMapHead().reverse == bits.Reverse64(k) && entry.PtrMapHead().conflict == conflict {
		entry.(MapItem).SetValue(item.Value())
		if btable.level > 0 && cnt > int(btable.len) {
			btable.len = int64(cnt)
		}
		return true
	}
	var pEntry HMapEntry
	var tStart *list_head.ListHead
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
		tStart = btable.start
	}

	item.PtrListHead().Init()
	if addOpt == nil {
		h.add2(tStart, item)
	} else {
		h.add2(tStart, item, addOpt)
	}
	atomic.AddInt64(&h.len, 1)
	if btable.level > 0 {
		atomic.AddInt64(&btable.len, 1)
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

type CondOfFinder func(ehead *entryHMap) bool

func CondOfFind(reverse uint64, l sync.Locker) CondOfFinder {

	return func(ehead *entryHMap) bool {

		if EnableStats {
			l.Lock()
			DebugStats[CntSearchEntry]++
			l.Unlock()
		}
		return reverse <= ehead.reverse
	}

}

var Failreverse uint64 = 0

func (h *Map) _get(k, conflict uint64) (MapItem, bool) {

	if EnableStats {
		h.mu.Lock()
		DebugStats[CntOfGet]++
		h.mu.Unlock()
	}
	var bucket *bucket
	switch h.modeForBucket {
	case CombineSearch, CombineSearch2, CombineSearch3:

		e := h.searchKey(k, true)
		if e == nil {
			if Failreverse == 0 {
				Failreverse = bits.Reverse64(k)
			}
			return nil, false
		}
		if e.PtrMapHead().reverse != bits.Reverse64(k) || e.PtrMapHead().conflict != conflict {
			return nil, false
		}
		return e.(MapItem), true

	default:
		bucket = h.searchBucket(k)
		break
	}

	if bucket == nil {
		return nil, false
	}
	var e *entryHMap

	if h.modeForBucket == FalsesSearchForBucket {
		return nil, true
	}

	if e == nil {
		return nil, false
	}
	if e.reverse != bits.Reverse64(k) || e.conflict != conflict {
		return nil, false
	}

	return e, true

}

func (h *Map) notHaveBuckets() bool {
	return h.lastBucket.Next().Prev().Empty()
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

	for cur := h.buckets.ListHead.DirectPrev().DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
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
func (h *Map) LoadItem(key interface{}) (MapItem, bool) {
	return h._get(KeyToHash(key))
}

func (h *Map) LoadItemByHash(k uint64, conflict uint64) (MapItem, bool) {
	return h._get(k, conflict)
}

// Set ... set the value for a key
func (h *Map) Set(key, value interface{}) bool {

	s := &SampleItem{
		K: key,
		V: value,
	}
	if _, ok := h.ItemFn().(*SampleItem); !ok {
		ItemFn(func() MapItem {
			return &EmptySampleHMapEntry
		})(h)
	}
	return h.StoreItem(s)
}

// StoreItem ... set key/value item with embedded-linked-list
func (h *Map) StoreItem(item MapItem) bool {
	k, conflict := KeyToHash(item.Key())
	return h._set(k, conflict, item)
}

func (h *Map) eachEntry(start *list_head.ListHead, fn func(*entryHMap)) {
	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead(cur)
		if e.key == nil {
			continue
		}
		fn(e)
	}
	return
}

func (h *Map) each(start *list_head.ListHead, fn func(key, value interface{})) {

	for cur := start; !cur.Empty(); cur = cur.Next() {
		e := entryHMapFromListHead(cur)
		fn(e.key, e.value)
	}
	return
}

// must renename to find
func (h *Map) find(start *list_head.ListHead, cond func(HMapEntry) bool, opts ...searchArg) (result HMapEntry, cnt int) {

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

		if conf.ignoreBucketEntry && e.Key() == nil {
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

func (h *Map) MakeBucket(ocur *list_head.ListHead, back int) {
	cur := ocur
	cur = cur.Prev()

	e := entryHMapFromListHead(cur)
	cBucket := h.searchBucket(bits.Reverse64(e.reverse))
	if cBucket == nil {
		return
	}
	nextBucket := bucketFromListHead(cBucket.Prev())

	newReverse := cBucket.reverse / 2
	if nextBucket.reverse == ^uint64(0) && cBucket.reverse == 0 {
		newReverse = bits.Reverse64(0x1)
	} else if nextBucket.reverse == ^uint64(0) {
		newReverse += ^uint64(0) / 2
		newReverse += 1
	} else {
		newReverse += nextBucket.reverse / 2
	}

	level := 0
	for cur := bits.Reverse64(newReverse); cur != 0; cur >>= 4 {
		level++
	}
	levelbuf := h.buf.fromLevel(level)
	if levelbuf == nil {
		if level <= 3 {
			h.buf.allocLevel(level)
		}
		if level > 3 {
			n := cBucket.PrevOnLevel()
			_ = n
			Log(LogWarn, "allocate big buffer?")
		}
	}
	var b *bucket
	if level <= 3 {
		b = h.buf.fromReverse(newReverse, level, false)
	} else {
		b = &bucket{}
	}
	if b == nil {
		Log(LogFatal, "cannot allocated level bucket buffer")
	}
	b.reverse, b.level, b.len = newReverse, level, 0

	if b.reverse == 0 && b.level > 1 {
		Log(LogWarn, "bucket.reverse = 0. but level 1= 1")
	}

	b.Init()
	b.LevelHead.Init()

	for cur := cBucket.start.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		b.len++
		e := entryHMapFromListHead(cur)
		if e.reverse > b.reverse {
			b.start = cur.DirectPrev()
			break
		}
	}

	atomic.AddInt64(&cBucket.len, -b.len)
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

	b.registerToUpLevel()

	if int(b.len) > h.maxPerBucket {
		h.MakeBucket(cBucket.start.Next(), int(b.len)/2)
	}
	if int(cBucket.len) > h.maxPerBucket {
		h.MakeBucket(nextBucket.start.Prev(), int(b.len)/2)
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

func (h *Map) add2(start *list_head.ListHead, e HMapEntry, opts ...HMethodOpt) bool {
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
		if !EnableStats || e.PtrMapHead().isDummy {
			return
		}

		if h.SearchKey(bits.Reverse64(e.PtrMapHead().reverse), ignoreBucketEntry(false)) == nil {
			sharedSearchOpt.Lock()
			sharedSearchOpt.e = errors.New("add: item is added. but not found")
			sharedSearchOpt.Unlock()
		}
	}()

	if pos != nil {
		pos.PtrListHead().InsertBefore(e.PtrListHead())
		if opt == nil || opt.bucket == nil {
			return true
		}
		btable := opt.bucket
		if btable != nil && e.(MapItem).Key() != nil && int(btable.len) > h.maxPerBucket {
			h.MakeBucket(e.PtrListHead(), int(btable.len)/2)
		}
		return true
	}
	if opt != nil && opt.bucket != nil {
		nextAsE(opt.bucket.entry(h)).PtrListHead().InsertBefore(e.PtrListHead())
		return true
	}
	h.last.InsertBefore(e.PtrListHead())
	return true
}

func (h *Map) DumpBucket() string {
	var b strings.Builder

	for cur := h.buckets.ListHead.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		btable := bucketFromListHead(cur)
		fmt.Fprintf(&b, "bucket{reverse: 0x%16x, len: %d, start: %p, level{%d, cur: %p, prev: %p next: %p} down: %p}\n",
			btable.reverse, btable.len, btable.start, btable.level, &btable.LevelHead, btable.LevelHead.DirectPrev(), btable.LevelHead.DirectNext(), btable.downLevel)
	}
	return b.String()
}

func (h *Map) DumpBucketPerLevel() string {
	var b strings.Builder

	for i := range h.levels {
		cBucket := h.levelBucket(i + 1)
		if cBucket == nil {
			continue
		}
		if h.isEmptyBylevel(i + 1) {
			continue
		}
		fmt.Fprintf(&b, "bucket level=%d\n", i+1)
		for cur := cBucket.LevelHead.DirectPrev().DirectNext(); !cur.Empty(); {
			cBucket = bucketFromLevelHead(cur)
			cur = cBucket.LevelHead.DirectNext()
			fmt.Fprintf(&b, "bucket{reverse: 0x%16x, len: %d, start: %p, level{%d, cur: %p, prev: %p next: %p} down: %p}\n",
				cBucket.reverse, cBucket.len, cBucket.start, cBucket.level, &cBucket.LevelHead, cBucket.LevelHead.DirectPrev(), cBucket.LevelHead.DirectNext(), cBucket.downLevel)

		}
	}
	return b.String()
}

func (h *Map) DumpEntry() string {
	var b strings.Builder

	for cur := h.start.Prev().Next(); !cur.Empty(); cur = cur.Next() {
		//var e HMapEntry
		//e = e.HmapEntryFromListHead(cur)
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
		e := fromMapHead(mhead)

		var ekey interface{}
		ekey = e.Key()
		fmt.Fprintf(&b, "entryHMap{key: %+10v, k: 0x%16x, reverse: 0x%16x), conflict: 0x%x, cur: %p, prev: %p, next: %p}\n",
			ekey, bits.Reverse64(mhead.reverse), mhead.reverse, mhead.conflict, mhead.PtrListHead(), mhead.PtrListHead().DirectPrev(), mhead.PtrListHead().DirectNext())
	}

	return b.String()
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

	empty := &entryHMap{
		key:   nil,
		value: nil,
	}
	empty.reverse, empty.conflict = nBtable.reverse, 0
	empty.PtrMapHead().isDummy = true
	empty.Init()
	var thead *list_head.ListHead
	if tBtable.Empty() {
		thead = h.start.Prev().Next()
	} else {
		tBucket := bucketFromListHead(tBtable)
		thead = tBucket.start.Prev().Next()
	}
	h.add2(thead, empty)
	tBtable.InsertBefore(&nBtable.ListHead)
	nBtable.start = &empty.ListHead
}

func (h *Map) addBucket(nBtable *bucket) {

	for bcur := h.buckets.ListHead.Prev().Next(); !bcur.Empty(); bcur = bcur.Next() {
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

func (h *Map) findNextLevelBucket(reverse uint64, level int) (cur *list_head.ListHead) {

	bcur := h.levelBucket(level)
	if bcur == nil {
		return nil
	}
	prevs := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	front := bcur.LevelHead.Front()
	list_head.DefaultModeTraverse.Option(prevs...)
	bcur = bucketFromLevelHead(front.DirectPrev().DirectNext())

	for cur := &bcur.LevelHead; true; cur = cur.DirectNext() {
		if cur.Empty() {
			return cur
		}
		bcur := bucketFromLevelHead(cur)
		if reverse > bcur.reverse {
			return &bcur.LevelHead
		}
	}
	return nil
}

func (h *Map) initLevels() {

	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.levels {
		b := &bucket{level: i + 1}
		b.LevelHead.InitAsEmpty()
		h.levels[i].Store(b)
	}
}

func (h *Map) setLevel(level int, b *bucket) bool {

	return false
}

func (h *Map) levelBucket(level int) (b *bucket) {
	ov := h.levels[level-1]
	b = ov.Load().(*bucket)

	return b
}

func (h *Map) isEmptyBylevel(level int) bool {
	if len(h.levels) < level {
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

	if mhead.isDummy {
		return entryHMapFromListHead(mhead.PtrListHead())
	}
	return SampleItemFromListHead(mhead.PtrListHead())
}

func nextNoCheck(e HMapEntry) HMapEntry {
	//return entryHMapFromListHead(e.ListHead.next)
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
	return h.buf.fromReverse(reverse, 1, true)

	// i := int(reverse >> (4 * 15))
	// if h.topBuckets[i] == nil {
	// 	return nil
	// }
	// return h.topBuckets[i]
}

func (h *Map) searchBucket4update(k uint64) (*bucket, uint64) {

	reverseNoMask := bits.Reverse64(k)

	for level := h.buf.maxLevel(); level > 0; level-- {
		cBuf := h.buf.fromReverse(reverseNoMask, level, true)
		if cBuf == nil {
			continue
		}
		if h.modeForBucket == CombineSearch {
			return h.searchBucketWithBucket(cBuf, reverseNoMask)
		} else {
			return cBuf, reverseNoMask
		}
	}
	if h.modeForBucket == CombineSearch {
		return h._searchBucket4update(k)
	} else {
		return nil, reverseNoMask
	}
}
func (h *Map) _searchBucket4update(k uint64) (*bucket, uint64) {
	reverseNoMask := bits.Reverse64(k)
	topLevelBucket := h.topLevelBucket(reverseNoMask)
	return h.searchBucketWithBucket(topLevelBucket, reverseNoMask)
}

func (h *Map) searchBucketWithBucket(topLevelBucket *bucket, reverseNoMask uint64) (*bucket, uint64) {

	if topLevelBucket == nil {
		return nil, reverseNoMask
	}
	var nearestCur *bucket
	nearestCur = topLevelBucket

	for lbCur, plbCur := topLevelBucket, topLevelBucket.PrevOnLevel(); lbCur != plbCur || lbCur.downLevel != nil || plbCur.downLevel != nil; lbCur, plbCur = plbCur, plbCur.PrevOnLevel() {
	RETRY:
		if EnableStats {
			h.mu.Lock()
			DebugStats[CntLevelBucket]++
			h.mu.Unlock()
		}
		if nearUint64(lbCur.reverse, nearestCur.reverse, reverseNoMask) != nearestCur.reverse {
			nearestCur = lbCur
		}
		if nearUint64(plbCur.reverse, nearestCur.reverse, reverseNoMask) != nearestCur.reverse {
			nearestCur = plbCur
		}
		if lbCur.reverse == reverseNoMask {
			goto EACH_ENTRY
		}
		if lbCur.reverse < reverseNoMask && plbCur.reverse < reverseNoMask && lbCur != plbCur {
			continue
		}
		if lbCur.downLevel != nil {
			plbCur = bucketFromLevelHead(lbCur.downLevel)
			lbCur = plbCur.NextOnLevel()
		} else if plbCur.downLevel != nil {
			plbCur = bucketFromLevelHead(plbCur.downLevel)
			lbCur = plbCur.NextOnLevel()
		} else {
			goto EACH_ENTRY
		}

		goto RETRY

	EACH_ENTRY:
		return nearestCur, reverseNoMask

	}
	return nearestCur, reverseNoMask
}

func (h *Map) searchBucket4Key4(k uint64, ignoreBucketEnry bool) (b *bucket, reverse uint64, ignore bool) {
	b, reverse = h.searchBucket4update(k)
	ignore = ignoreBucketEnry
	return
}

func (h *Map) searchBucket4Key3(k uint64, ignoreBucketEnry bool) (*bucket, uint64, bool) {

	reverseNoMask := bits.Reverse64(k)
	topLevelBucket := h.topLevelBucket(reverseNoMask)
	//gTopLevelBucket := topLevelBucket.PrevOnLevel()
	if topLevelBucket == nil {
		return nil, reverseNoMask, ignoreBucketEnry
	}
	var upCur *bucket

	for lbCur := topLevelBucket; true; lbCur = lbCur.PrevOnLevel() {
	RETRY:
		if EnableStats && ignoreBucketEnry {
			h.mu.Lock()
			DebugStats[CntLevelBucket]++
			h.mu.Unlock()
		}
		if lbCur.reverse < reverseNoMask && lbCur != lbCur.PrevOnLevel() {
			continue
		}
		upCur = lbCur.NextOnLevel()
		if upCur.downLevel == nil {
			if nearUint64(lbCur.reverse, upCur.reverse, reverseNoMask) != lbCur.reverse {
				lbCur = upCur
			}
			goto EACH_ENTRY
		}
		lbCur = bucketFromLevelHead(upCur.downLevel)
		goto RETRY

	EACH_ENTRY:
		if upCur != nil && nearUint64(upCur.reverse, lbCur.reverse, reverseNoMask) == upCur.reverse {
			return upCur, reverseNoMask, ignoreBucketEnry
		} else {
			return lbCur, reverseNoMask, ignoreBucketEnry
		}

	}
	return nil, 0, ignoreBucketEnry
}

func nearBucketFromCache(levels [16]*bucket, lbNext *bucket, reverseNoMask uint64) (result *bucket) {
	noNil := true
	result = lbNext
	for i, b := range levels {
		if b == nil {
			noNil = false
			break
		}
		if i+1 == result.level {
			continue
		}
		//b := (*bucket)(unsafe.Pointer(p))
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
			if ignoreBucketEnry && cur.isDummy {
				continue
			}
			//curReverse := bits.Reverse64(cur.k)
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
		//curReverse := bits.Reverse64(cur.k)
		curReverse := cur.reverse
		if ignoreBucketEnry && cur.isDummy {
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

	for cur := h.start.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
		if mhead.isDummy {
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
