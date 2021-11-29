// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package skitlistmap is concurrent map implementatin
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
	LenearSearchForBucket SearchMode = 0
	ReversSearchForBucket            = 1
	NestedSearchForBucket            = 2
	CombineSearch                    = 3
	CombineSearch2                   = 4

	NoItemSearchForBucket = 9 // test mode
	FalsesSearchForBucket = 10
)

// Map ... Skip List Map is an ordered and concurrent map.
// this Map is gourtine safety for reading/updating/deleting, require locking and coordination. This
type Map struct {
	buckets    bucket
	lastBucket *list_head.ListHead
	topBuckets [16]*bucket

	len          int64
	maxPerBucket int
	start        *list_head.ListHead
	last         *list_head.ListHead

	modeForBucket SearchMode
	mu            sync.Mutex
	levelCache    [16]atomic.Value

	ItemFn func() MapItem
}

type LevelHead list_head.ListHead

type bucket struct {
	level   int
	reverse uint64
	len     int64
	start   *list_head.ListHead // to MapEntry

	downLevel *list_head.ListHead // to bucket.Levelhead

	LevelHead list_head.ListHead // to same level bucket
	list_head.ListHead
}

func (e *bucket) Offset() uintptr {
	return unsafe.Offsetof(e.ListHead)
}

func (e *bucket) OffsetLevel() uintptr {
	return unsafe.Offsetof(e.LevelHead)
}

func (e *bucket) PtrListHead() *list_head.ListHead {
	return &e.ListHead
}

func (e *bucket) PtrLevelHead() *list_head.ListHead {
	return &e.LevelHead
}

func (e *bucket) FromListHead(head *list_head.ListHead) list_head.List {
	return entryHMapFromListHead(head)
}

func bucketFromListHead(head *list_head.ListHead) *bucket {
	return (*bucket)(list_head.ElementOf(emptyBucket, head))
}

func bucketFromLevelHead(head *list_head.ListHead) *bucket {
	if head == nil {
		return nil
	}
	return (*bucket)(unsafe.Pointer(uintptr(unsafe.Pointer(head)) - emptyBucket.OffsetLevel()))
}

type entryHMap struct {
	key   interface{}
	value interface{}
	//k        uint64
	//reverse  uint64
	//conflict uint64
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
}

// func (e *entryHMap) Offset() uintptr {
// 	return unsafe.Offsetof(e.ListHead)
// }

// func (e *entryHMap) PtrListHead() *list_head.ListHead {
// 	return &e.ListHead
// }

// func (e *entryHMap) FromListHead(head *list_head.ListHead) List {
// 	return entryHMapFromListHead(head)
// }

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

	hmap.initLevelCache()

	return hmap
}

// func (h *HMap) set(key, value interface{}) bool {
// 	k, conflict := KeyToHash(key)
// 	return h._set(k, conflict, key, value)
// }

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

	for i := range topReverses {
		reverse := topReverses[i]
		btable = &bucket{
			level: 1,
			len:   0,
		}
		btable.reverse = reverse
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
		levelBucket = h.levelBucket(btable.level)
		levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
		h.setLevel(btable.level, levelBucket)
		h.topBuckets[i] = btable
	}

	// btable = &bucket{
	// 	level: 1,
	// 	len:   0,
	// }
	// btable.reverse = 0
	// btable.Init()
	// btable.LevelHead.Init()

	// empty = &entryHMap{
	// 	key:   nil,
	// 	value: nil,
	// }
	// empty.reverse, empty.conflict = btable.reverse, 0
	// empty.PtrMapHead().isDummy = true

	// empty.Init()
	// btablefirst.start.InsertBefore(&empty.ListHead)

	// btablefirst.Next().InsertBefore(&btable.ListHead)
	// btable.start = &empty.ListHead
	// levelBucket = h.levelBucket(btable.level)
	// levelBucket.LevelHead.DirectPrev().DirectNext().InsertBefore(&btable.LevelHead)
	// h.setLevel(btable.level, levelBucket)

	// if EnableStats {
	// 	fmt.Printf("%s\n", h.DumpBucket())
	// 	fmt.Printf("%s\n", h.DumpEntry())
	// }
}

//FIXME: renate _set
func (h *Map) _set2(k, conflict uint64, item MapItem) bool {

	item.PtrMapHead().reverse = bits.Reverse64(k)
	item.PtrMapHead().conflict = conflict
	//item.PtrMapHead().reverse = bits.Reverse64(k)

	h.initBeforeSet()

	var btable *bucket
	var addOpt HMethodOpt
	_ = addOpt

	if h.modeForBucket != CombineSearch {
		btable = h.searchBucket(k)
	} else {
		btable = h.searchBucket4update(k)

		if btable.reverse > item.PtrMapHead().reverse {
			btable = btable.NextOnLevel()
			if btable.reverse > item.PtrMapHead().reverse {
				btable = h.searchBucket(k)
			}
		}
	}

	if btable != nil && btable.start == nil {
		_ = ""
	}
	if btable == nil || btable.start == nil {
		btable = &bucket{}
		btable.start = h.start.Prev().Next()
	} else {
		addOpt = WithBucket(btable)
	}

	entry, cnt := h.find2(btable.start, func(item HMapEntry) bool {
		return bits.Reverse64(k) <= item.PtrMapHead().reverse
	})
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

		if pEntry.PtrMapHead().reverse < bits.Reverse64(k) {
			tStart = pEntry.PtrListHead()
		} else {
			_ = ""
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

//Deprecated: must use _set
func (h *Map) _set(k, conflict uint64, key, value interface{}) bool {

	h.initBeforeSet()

	var btable *bucket
	var addOpt HMethodOpt

	btable = h.searchBucket(k)
	if btable != nil && btable.start == nil {
		_ = ""
	}
	if btable == nil || btable.start == nil {
		btable = &bucket{}
		oldConfs := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
		btable.start = h.start.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM())
		list_head.DefaultModeTraverse.Option(oldConfs...)
	} else {
		addOpt = WithBucket(btable)
	}

	entry, cnt := h.find(btable.start, func(ehead *entryHMap) bool {
		return bits.Reverse64(k) <= ehead.reverse && ehead.key != nil
	})

	if entry != nil && entry.reverse == bits.Reverse64(k) && entry.conflict == conflict {
		entry.value = value
		if btable.level > 0 && cnt > int(btable.len) {
			btable.len = int64(cnt)
		}
		return true
	}
	var pEntry *entryHMap
	var tStart *list_head.ListHead

	if entry != nil {
		pEntry = entryHMapFromListHead(entry.Prev().PtrListHead())
		erk := entry.reverse
		prk := pEntry.reverse
		rk := bits.Reverse64(k)
		_, _, _ = erk, prk, rk

		if pEntry.reverse < bits.Reverse64(k) {
			tStart = &pEntry.ListHead
		} else {
			_ = ""
		}
	}

	if tStart == nil {
		tStart = btable.start
	}

	entry = &entryHMap{
		key:   key,
		value: value,
	}
	entry.reverse, entry.conflict = bits.Reverse64(k), conflict
	entry.Init()
	if addOpt == nil {
		h.add(tStart, entry)
	} else {
		h.add(tStart, entry, addOpt)
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

	// if e := h.search(KeyToHash(key)); e != nil {
	// 	return e.value, true
	// }

	// return nil, false
	//bucket := h.searchBucket(k)
	if EnableStats {
		h.mu.Lock()
		DebugStats[CntOfGet]++
		h.mu.Unlock()
	}
	var ebucket *bucket
	var bucket *bucket
	switch h.modeForBucket {
	case FalsesSearchForBucket:
		bucket = h.searchBucket4(k)
		break
	case NoItemSearchForBucket:
		bucket = h.searchBucket4(k)
		return nil, true

	case NestedSearchForBucket:
		bucket = h.searchBucket4(k)

		break
	case CombineSearch, CombineSearch2:

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

	case ReversSearchForBucket:
		bucket = h.searchBucket2(k)
		break
	default:
		bucket = h.searchBucket(k)
		break
	}

	//bucket := h.rsearchBucket(k)
	// _ = b2

	//return nil, false

	if bucket == nil {
		return nil, false
	}
	rk := bits.Reverse64(k)
	_ = rk
	var e *entryHMap
	useBsearch := false
	useReverse := false

	if !useBsearch {

		if bucket.DirectPrev().Empty() {
			useReverse = false
		} else if useReverse {
			ebucket = bucketFromListHead(bucket.DirectPrev())
		}
		if useReverse && nearUint64(bucket.reverse, ebucket.reverse, bits.Reverse64(k)) == ebucket.reverse {
			e, _ = h.reverse(ebucket.start, func(ehead *entryHMap) bool {
				return rk <= ehead.reverse
			})
		} else {

			if bucket.reverse > bits.Reverse64(k) {
				e, _ = h.reverse(bucket.start, func(ehead *entryHMap) bool {
					return rk <= ehead.reverse
				})

			} else {
				e, _ = h.find(bucket.start, CondOfFind(rk, &h.mu))
				// cnt := 0
				// for cur := bucket.start; !cur.Empty(); cur = cur.DirectNext() {
				// 	e = entryHMapFromListHead(cur)
				// 	if rk <= bits.Reverse64(e.k) {
				// 		break
				// 	}
				// 	cnt++
				// }

			}
		}
	}
	if h.modeForBucket == FalsesSearchForBucket {
		return nil, true
	}

	if useBsearch {
		e, _ = h.bsearch(bucket, func(ehead *entryHMap) bool {
			return rk <= ehead.reverse
		})
	}
	// _ = e2

	// if e != e2 {
	// 	_ = "???"
	// }

	if e == nil {
		return nil, false
	}
	if e.reverse != bits.Reverse64(k) || e.conflict != conflict {
		return nil, false
	}
	//return nil, false

	return e, true

}

// func (h *HMap) search(k, conflict uint64) *entryHMap {

// 	for cur := h.buckets[k%cntOfHampBucket].Prev(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
// 		e := entryHMapFromListHead(cur)
// 		if e.k == k && e.conflict == conflict {
// 			return e
// 		}
// 	}
// 	return nil
// }

// func (h *HMap) bucketEnd() (result *bucket) {

// 	for cur := h.buckets.ListHead.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
// 		result = bucketFromListHead(cur)
// 	}
// }
func (h *Map) notHaveBuckets() bool {
	return h.lastBucket.Next().Prev().Empty()
}
func (h *Map) searchBucket2(k uint64) (result *bucket) {

	if nearUint64(0, ^uint64(0), k) != 0 {
		return h.searchBucket(k)
	}
	return h.rsearchBucket(k)
}

func levelMask(level int) (mask uint64) {
	mask = 0
	for i := 0; i < level; i++ {
		mask = (mask << 4) | 0xf
	}
	return
}

func (h *Map) searchBucket4(k uint64) (result *bucket) {

	level := 1
	levelbucket := bucketFromLevelHead(h.levelBucket(level).LevelHead.DirectPrev().DirectNext())

	var pCur, nCur *bucket

	// cur = bucketFromLevelHead(cur.LevelHead.next)
	for cur := levelbucket; !cur.Empty(); {
		if EnableStats {
			h.mu.Lock()
			DebugStats[CntSearchBucket]++
			h.mu.Unlock()
		}

		cReverseNoMask := bits.Reverse64(k)
		_ = cReverseNoMask
		cReverse := bits.Reverse64(k & toMask(cur.level))
		if cReverse == cur.reverse {
			level++
			if EnableStats {
				h.mu.Lock()
			}
			nCur = FindBucketWithLevel2(&cur.ListHead, bits.Reverse64(k), level)
			if EnableStats {
				h.mu.Unlock()
			}
			if nCur == nil {
				return cur
			}
			if bits.Reverse64(k&toMask(nCur.level)) != nCur.reverse {
				return cur
			}
			cur = nCur
			level = nCur.level
			continue
		}
		if !cur.LevelHead.DirectPrev().Empty() {
			pCur = bucketFromLevelHead(cur.LevelHead.DirectPrev())
			if pCur.reverse > cReverse && cReverse > cur.reverse {
				return pCur
			}
		}
		if !cur.LevelHead.DirectNext().Empty() {
			nCur = bucketFromLevelHead(cur.LevelHead.DirectNext())
			if cur.reverse > cReverse && cReverse > nCur.reverse {
				return cur
			}
		}

		if cReverse < cur.reverse {
			if cur.LevelHead.DirectNext().Empty() {
				_ = "???"
			}
			cur = bucketFromLevelHead(cur.LevelHead.DirectNext())
			continue
		}
		if cur.LevelHead.DirectPrev().Empty() {
			return cur
		}
		cur = bucketFromLevelHead(cur.LevelHead.DirectPrev())

	}
	return nil
}

func (h *Map) searchBucket3(k uint64) (result *bucket) {

	level := 1
	var bcur *bucket
	for cur := h.buckets.ListHead.DirectPrev().DirectNext(); !cur.Empty(); {
		bcur = bucketFromListHead(cur)
		blevel := FindBucketWithLevel(cur, bits.Reverse64(k), level)
		if blevel != nil {
			cur = &blevel.ListHead
			level++
			continue
		}
		for cur := cur; !cur.Empty(); cur = cur.DirectNext() {
			bcur = bucketFromListHead(cur)
			if bits.Reverse64(k) > bcur.reverse {
				return bcur
			}
		}
		return nil
	}
	return nil
}

func FindBucketWithLevel2(chead *list_head.ListHead, reverse uint64, level int) *bucket {

	cBucket := bucketFromListHead(chead)
	if cBucket == nil {
		return nil
	}

	// バケットはつねに reverse より小さい
	if cBucket.reverse > reverse {
		if cBucket.LevelHead.DirectNext().Empty() {
			return nil
		}
		if !cBucket.LevelHead.DirectNext().Empty() {
			cBucket = bucketFromLevelHead(cBucket.LevelHead.DirectNext())
		}
	}

	pBucket := bucketFromLevelHead(cBucket.LevelHead.DirectPrev())
	var mReverse uint64
	var nCur *bucket
	for cur := &cBucket.ListHead; !cur.Empty() && cur != &pBucket.ListHead; {
		if EnableStats {
			//h.mu.Lock()
			DebugStats[CntLevelBucket]++
			//h.mu.Unlock()
		}
		cBucket = bucketFromListHead(cur)
		mReverse = (bits.Reverse64(toMask(cBucket.level)) & reverse)
		if cBucket.reverse == 0 {
			return nil
		}
		if cBucket.level != level {

			if cBucket.reverse > reverse {
				_ = "invalid"
			}
			cur = cur.DirectPrev()
			continue
		}
		if mReverse == cBucket.reverse {
			return cBucket
		}
		if cBucket.reverse > mReverse {
			if cBucket.LevelHead.DirectNext().Empty() {
				return nil
			}
			nCur = bucketFromLevelHead(cBucket.LevelHead.DirectNext())
			if nCur.reverse < mReverse {
				return nCur
			}
			//return bucketFromLevelHead(cBucket.LevelHead.next)
			return nil
		}

		if cBucket.LevelHead.DirectPrev().Empty() {
			return cBucket
		}

		cur = &bucketFromLevelHead(cBucket.LevelHead.DirectPrev()).ListHead
	}
	return nil
}

func FindBucketWithLevel(chead *list_head.ListHead, reverse uint64, level int) *bucket {

	cur := bucketFromListHead(chead)
	if cur == nil {
		return nil
	}
	cnt := -1
	for cur != nil && !cur.Empty() {
		cnt++
		if cur.reverse == 0 {
			return nil
		}
		if cur.level != level {

			if (bits.Reverse64(toMask(cur.level)) & reverse) == cur.reverse {
				if !cur.DirectPrev().Empty() {
					cur = bucketFromListHead(cur.DirectPrev())
					continue
				}
			}
			if (bits.Reverse64(toMask(cur.level)) & reverse) > cur.reverse {
				return bucketFromLevelHead(cur.LevelHead.DirectPrev())
			}

			chead2 := cur.DirectNext()
			if chead2 == nil || chead2.Empty() {
				cur = nil
				return nil
			}

			nCur := bucketFromListHead(chead2)
			if (bits.Reverse64(toMask(nCur.level)) & reverse) == nCur.reverse {
				return nil
			}
			cur = nCur
			continue
		}
		cReverse := (bits.Reverse64(toMask(level)) & reverse)
		if cReverse == cur.reverse {
			return cur
		}

		if cReverse > cur.reverse {
			if !cur.LevelHead.DirectPrev().Empty() {
				cur = bucketFromLevelHead(cur.LevelHead.DirectPrev())
				if cReverse < cur.reverse {
					return cur
				}
				continue
			}
			return nil
			//return bucketFromLevelHead(cur.LevelHead.DirectPrev())
		}
		if cur.LevelHead.Empty() || cur.LevelHead.DirectNext().Empty() {
			return nil
		}
		pcur := bucketFromLevelHead(cur.LevelHead.DirectPrev())
		ncur := bucketFromLevelHead(cur.LevelHead.DirectNext())
		_, _ = pcur, ncur

		cur = ncur
		_ = cur
	}
	return nil
}

func (h *Map) searchBucket(k uint64) (result *bucket) {
	cnt := 0

	for cur := h.buckets.ListHead.DirectPrev().DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
		//for cur := h.buckets.ListHead.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
		bcur := bucketFromListHead(cur)
		if bits.Reverse64(k) > bcur.reverse {
			return bcur
		}
		cnt++
	}
	return
}

func (h *Map) rsearchBucket(k uint64) (result *bucket) {
	cnt := 0

	for cur := h.lastBucket.DirectNext().DirectPrev(); !cur.Empty(); cur = cur.DirectPrev() {
		//for cur := h.buckets.ListHead.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM()); !cur.Empty(); cur = cur.Next(list_head.WaitNoM()) {
		bcur := bucketFromListHead(cur)
		if bits.Reverse64(k) <= bcur.reverse {
			return bucketFromListHead(bcur.DirectNext())
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
	return h._set2(k, conflict, item)
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
func (h *Map) find2(start *list_head.ListHead, cond func(HMapEntry) bool, opts ...searchArg) (result HMapEntry, cnt int) {

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

//Deprecated: rewrite to find2
func (h *Map) find(start *list_head.ListHead, cond func(*entryHMap) bool, opts ...searchArg) (result *entryHMap, cnt int) {

	conf := sharedSearchOpt
	previous := conf.Options(opts...)
	defer func() {
		if previous != nil {
			conf.Options(previous)
		}
	}()

	cnt = 0
	var e *entryHMap
	if start.Empty() {
		return
	}

	for cur := start; cur != cur.DirectNext(); cur = cur.DirectNext() {
		e = entryHMapFromListHead(cur)
		// erk := bits.Reverse64(e.k)
		// _ = erk
		if conf.ignoreBucketEntry && e.key == nil {
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

func (h *Map) reverse(start *list_head.ListHead, cond func(*entryHMap) bool) (result *entryHMap, cnt int) {

	cnt = 0

	for cur := start; !cur.Empty(); cur = cur.DirectPrev() {
		e := entryHMapFromListHead(cur)
		// erk := bits.Reverse64(e.k)
		// _ = erk
		if !cond(e) {
			result = entryHMapFromListHead(cur.DirectNext())
			return
		}
		cnt++
	}
	return nil, cnt
}

func middleListHead(oBegin, oEnd *list_head.ListHead) (middle *list_head.ListHead) {
	// begin := oBegin
	// end := oEnd
	b := oBegin.Empty()
	e := oEnd.Empty()
	_, _ = b, e
	cnt := 0
	for begin, end := oBegin, oEnd; !begin.Empty() && !end.Empty(); begin, end = begin.DirectNext(), end.DirectPrev() {
		if begin == end {
			return begin
		}
		if begin.DirectPrev() == end {
			return begin
		}
		cnt++
	}
	return
}

func bsearchListHead(oBegin, oEnd *list_head.ListHead, cond func(*list_head.ListHead) bool) *list_head.ListHead {
	begin := oBegin
	end := oEnd
	middle := middleListHead(begin, end)
	for {
		if middle == nil {
			return nil
		}

		if cond(begin) {
			return begin
		}
		if cond(middle) {
			end = middle
			middle = middleListHead(begin, end)
			if end == middle {
				return middle
			}
			continue
		}
		if !cond(end) {
			return end
		}
		if begin == middle {
			return end
		}
		begin = middle
		middle = middleListHead(begin, end)
	}

}

func absDiffUint64(x, y uint64) uint64 {
	if x < y {
		return y - x
	}
	return x - y
}

func nearUint64(a, b, dst uint64) uint64 {
	if absDiffUint64(a, dst) > absDiffUint64(b, dst) {
		return b
	}
	return a

}

func (h *Map) bsearch(sbucket *bucket, cond func(*entryHMap) bool) (result *entryHMap, cnt int) {
	if sbucket.Empty() || sbucket.DirectPrev().Empty() {
		return nil, 0
	}

	ebucket := bucketFromListHead(sbucket.DirectPrev())
	if sbucket.start.DirectPrev().DirectNext().Empty() || ebucket.start.DirectPrev().Empty() {
		return nil, 0
	}

	rhead := bsearchListHead(sbucket.start.DirectPrev().DirectNext(), ebucket.start.DirectPrev(), func(cur *list_head.ListHead) bool {
		e := entryHMapFromListHead(cur)
		return cond(e)
	})
	if rhead == nil {
		return nil, 0
	}
	result = entryHMapFromListHead(rhead)
	return

	// cnt = 0

	// for cur := start; !cur.Empty(); cur = cur.DirectNext() {
	// 	e := entryHMapFromListHead(cur)
	// 	if cond(e) {
	// 		result = e
	// 		return
	// 	}
	// 	cnt++
	// }
	// return nil, cnt
}

func (b *bucket) registerToUpLevel() {

	if b.level > 1 {
		var upBucket *bucket
		upBucket = nil
		_ = upBucket
		if nextBuckketOnLevel := b.NextOnLevel(); nextBuckketOnLevel != b {
			for cur := b; cur.reverse > nextBuckketOnLevel.reverse; cur = cur.nextAsB() {
				if cur == cur.nextAsB() {
					break
				}
				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
			}
		} else if nextBucket := b.nextAsB(); nextBucket != b {
			for cur := nextBucket; cur.level != b.level; cur = cur.nextAsB() {
				if cur == cur.nextAsB() {
					break
				}
				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
			}
		} else {

			for cur := b.prevAsB(); cur.level != b.level; cur = cur.prevAsB() {
				if cur == cur.prevAsB() {
					break
				}
				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
			}
		}

		if upBucket != nil {
			upBucket.downLevel = &b.LevelHead
		}

	}

}

func (h *Map) MakeBucket(ocur *list_head.ListHead, back int) {
	cur := ocur
	//for i := 0; i < 2; i++ {
	cur = cur.Prev()
	//}

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

	// mask := ^uint64(0)
	// for ecur := cBucket.entry(h); true; ecur = ecur.Next() {
	// 	ecReverse := bits.Reverse64(ecur.PtrMapHead().k)
	// 	if ecReverse >= newReverse {
	// 		break
	// 	}
	// 	if ecur.PtrMapHead().isDummy {
	// 		continue
	// 	}
	// 	mask &= ecReverse
	// }

	b := &bucket{
		reverse: newReverse,
		level:   0,
		len:     0,
	}
	for cur := bits.Reverse64(b.reverse); cur != 0; cur >>= 4 {
		b.level++
	}
	if b.reverse == 0 && b.level > 1 {
		fmt.Printf("invalid")
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
	//cBucket.len -= nBucket.len
	atomic.AddInt64(&cBucket.len, -b.len)
	h.addBucket(b)

	nextLevel := h.findNextLevelBucket(b.reverse, b.level)

	if b.LevelHead.DirectNext() == &b.LevelHead {
		_ = "broken"
	}

	if nextLevel != nil {

		nextLevelBucket := bucketFromLevelHead(nextLevel)
		if nextLevelBucket.reverse < b.reverse {
			nextLevel.InsertBefore(&b.LevelHead)
		} else if nextLevelBucket.reverse != b.reverse {
			//nBucket.LevelHead.InsertBefore(nextLevel)

			nextnextBucket := bucketFromLevelHead(nextLevel.Next())
			_ = nextnextBucket
			nextLevel.DirectNext().InsertBefore(&b.LevelHead)
		}

		//nextLevel.InsertBefore(&nBucket.LevelHead)
		var nNext, nPrev *bucket
		if !b.LevelHead.DirectPrev().Empty() {
			nPrev = bucketFromLevelHead(b.LevelHead.Prev())
		}
		if !b.LevelHead.DirectNext().Empty() {
			nNext = bucketFromLevelHead(b.LevelHead.Next())
		}
		_, _ = nNext, nPrev

	} else {
		_ = "???"
	}
	if b.LevelHead.Next() == &b.LevelHead {
		_ = "broken"
	}
	// set upBucket.down = nBucket
	b.registerToUpLevel()

	// nextLeveByCache := bucketFromLevelHead(h.levelBucket(nBucket.level).LevelHead.DirectPrev().next)
	// _ = nextLeveByCache

	// if nextLeveByCache.LevelHead.DirectPrev().Empty() && nextLeveByCache.LevelHead.DirectNext().Empty() {
	// 	nextLeveByCache.LevelHead.DirectNext().InsertBefore(&nBucket.LevelHead)
	// 	o := h.levelBucket(nBucket.level)
	// 	_ = o
	// 	h.setLevel(nBucket.level, nextLeveByCache)
	// }

	// er := h.checklevelAll()
	// _ = er

	// if h.levelBucket(nBucket.level) == nil {
	// 	h.setLevel(nBucket.level, nBucket)
	// }

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
	pos, _ := h.find2(start, func(ehead HMapEntry) bool {
		cnt++
		return e.PtrMapHead().reverse < ehead.PtrMapHead().reverse
	}, ignoreBucketEntry(false))

	defer func() {
		if h.isEmptyBylevel(1) {
			return
		}

		if h.SearchKey(bits.Reverse64(e.PtrMapHead().reverse), ignoreBucketEntry(false)) == nil {
			sharedSearchOpt.Lock()
			sharedSearchOpt.e = errors.New("add: item is added. but not found")
			sharedSearchOpt.Unlock()
			// fmt.Printf("%s\n", h.DumpBucket())
			// fmt.Printf("%s\n", h.DumpBucketPerLevel())
			// fmt.Printf("%s\n", h.DumpEntry())
			// h.searchKey(e.k, ignoreBucketEntry(false))
			// fmt.Printf("fail store")
		}
	}()

	if pos != nil {
		pos.PtrListHead().InsertBefore(e.PtrListHead())
		if opt == nil || opt.bucket == nil {
			return true
		}
		btable := opt.bucket
		if btable != nil && e.(MapItem).Key() != nil && int(btable.len) > h.maxPerBucket {
			//if cnt > h.maxPerBucket && pos.key != nil {
			h.MakeBucket(e.PtrListHead(), int(btable.len)/2)
		}
		return true
	}
	if opt != nil && opt.bucket != nil {
		//opt.bucket.start.InsertBefore(&e.ListHead)
		nextAsE(opt.bucket.entry(h)).PtrListHead().InsertBefore(e.PtrListHead())
		return true
	}
	h.last.InsertBefore(e.PtrListHead())
	return true
}

func (h *Map) add(start *list_head.ListHead, e *entryHMap, opts ...HMethodOpt) bool {
	var opt *hmapMethod
	if len(opts) > 0 {
		opt = &hmapMethod{}
		for _, fn := range opts {
			fn(opt)
		}
	}

	cnt := 0
	pos, _ := h.find(start, func(ehead *entryHMap) bool {
		cnt++
		return e.reverse < ehead.reverse
	}, ignoreBucketEntry(false))

	defer func() {
		if h.isEmptyBylevel(1) {
			return
		}

		if h.SearchKey(bits.Reverse64(e.reverse), ignoreBucketEntry(false)) == nil {
			sharedSearchOpt.Lock()
			sharedSearchOpt.e = errors.New("add: item is added. but not found")
			sharedSearchOpt.Unlock()
			// fmt.Printf("%s\n", h.DumpBucket())
			// fmt.Printf("%s\n", h.DumpBucketPerLevel())
			// fmt.Printf("%s\n", h.DumpEntry())
			// h.searchKey(e.k, ignoreBucketEntry(false))
			// fmt.Printf("fail store")
		}
	}()

	if pos != nil {
		pos.InsertBefore(&e.ListHead)
		if opt == nil || opt.bucket == nil {
			return true
		}
		btable := opt.bucket
		if btable != nil && e.key != nil && int(btable.len) > h.maxPerBucket {
			//if cnt > h.maxPerBucket && pos.key != nil {
			h.MakeBucket(&e.ListHead, int(btable.len)/2)
		}
		return true
	}
	if opt != nil && opt.bucket != nil {
		//opt.bucket.start.InsertBefore(&e.ListHead)
		nextAsE(opt.bucket.entry(h)).PtrListHead().InsertBefore(&e.ListHead)
		return true
	}
	h.last.InsertBefore(&e.ListHead)

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

	for i := range h.levelCache {
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
	// a := b.String()
	// _ = a
	// fmt.Printf("!!!%s!!!!\n", b.String())
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

	for i := 0; i < level; i++ {
		if mask == 0 {
			mask = 0xf << (16 - level) * 4
			continue
		}
		mask |= 0xf << (16 - level) * 4
	}
	return
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
	// h.start.Prev(list_head.WaitNoM()).Next(list_head.WaitNoM())
	h.add2(thead, empty)
	tBtable.InsertBefore(&nBtable.ListHead)
	nBtable.start = &empty.ListHead
}

func (h *Map) addBucket(nBtable *bucket) {

	bstart := h.buckets.ListHead.Prev().Next()
RETRY:
	for bcur := h.buckets.ListHead.Prev().Next(); !bcur.Empty(); bcur = bcur.Next() {
		cBtable := bucketFromListHead(bcur)
		if cBtable.reverse == nBtable.reverse {
			return
		}

		if cBtable.reverse < nBtable.reverse {
			h._InsertBefore(&cBtable.ListHead, nBtable)
			//cBtable.InsertBefore(&nBtable.ListHead)
			if nBtable.reverse <= cBtable.reverse {
				_ = "???"
			}
			return
		}
	}

	bstart = h.buckets.ListHead.Prev().Next()
	breverse := bucketFromListHead(bstart).reverse
	_ = breverse
	bbrev := bucketFromListHead(h.lastBucket.Next().Prev())
	_ = bbrev
	if nBtable.reverse <= bucketFromListHead(bstart).reverse {
		if bbrev.reverse > nBtable.reverse {
			//bbrev.Next().InsertBefore(&nBtable.ListHead)
			h._InsertBefore(bbrev.Next(), nBtable)
			return
		} else {
			_ = "???"
			goto RETRY
		}
	}
	//bstart.InsertBefore(&nBtable.ListHead)
	h._InsertBefore(bstart, nBtable)
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

func (h *Map) initLevelCache() {

	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.levelCache {
		b := &bucket{level: i + 1}
		b.LevelHead.InitAsEmpty()
		//b.InitAsEmpty()
		h.levelCache[i].Store(b)
	}
}

func (h *Map) setLevel(level int, b *bucket) bool {

	return false
	// if len(h.levelCache) <= level-1 {
	// 	return false
	// }

	// //ov := h.levelCache[level-1]
	// obucket := h.levelCache[level-1].Load().(*bucket)
	// success := h.levelCache[level-1].CompareAndSwap(obucket, b)
	// return success
}

func (h *Map) levelBucket(level int) (b *bucket) {
	ov := h.levelCache[level-1]
	b = ov.Load().(*bucket)

	return b
}

func (h *Map) isEmptyBylevel(level int) bool {
	if len(h.levelCache) < level {
		return true
	}
	b := h.levelBucket(level)

	if b.Empty() {
		return true
	}
	// if b.LevelHead.DirectPrev().Empty() && b.LevelHead.DirectNext().Empty() {
	// 	return true
	// }
	prev := b.LevelHead.DirectPrev()
	next := b.LevelHead.DirectNext()

	if prev == prev.DirectPrev() && next == next.DirectNext() {
		return true
	}
	return false
}

func (h *Map) checklevelAll() error {

	for i := range h.levelCache {
		b := h.levelBucket(i + 1)
		if err := b.checklevel(); err != nil {
			return err
		}
	}
	return nil

}

func (b *bucket) checklevel() error {

	level := -1
	var reverse uint64
	for cur := b.LevelHead.DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
		b := bucketFromLevelHead(cur)
		if level == -1 {
			level = bucketFromLevelHead(cur).level
			reverse = b.reverse
			continue
		}
		if level != bucketFromLevelHead(cur).level {
			return errors.New("invalid level")
		}
		if reverse < b.reverse {
			return errors.New("invalid reverse")
		}
		reverse = b.reverse
	}
	level = -1
	for cur := b.LevelHead.DirectPrev(); !cur.Empty(); cur = cur.DirectPrev() {
		b := bucketFromLevelHead(cur)
		if level == -1 {
			level = bucketFromLevelHead(cur).level
			reverse = b.reverse
			continue
		}
		if level != bucketFromLevelHead(cur).level {
			return errors.New("invalid level")
		}
		if reverse > b.reverse {
			return errors.New("invalid reverse")
		}
		reverse = b.reverse
	}
	return nil

}

type statKey byte

var EnableStats bool = false

const (
	CntSearchBucket  statKey = 1
	CntLevelBucket   statKey = 2
	CntSearchEntry   statKey = 3
	CntReverseSearch statKey = 4
	CntOfGet         statKey = 5
)

var DebugStats map[statKey]int = map[statKey]int{}

func (b *bucket) nextAsB() *bucket {
	//if b.ListHead.DirectNext().Empty() {
	next := b.ListHead.Next()
	if next == next.DirectNext() {
		//if next.Empty() {
		return b
	}
	//return bucketFromListHead(b.ListHead.DirectNext())
	return bucketFromListHead(next)

}

func (b *bucket) prevAsB() *bucket {

	//if b.ListHead.DirectPrev().Empty() {
	prev := b.ListHead.Prev()
	if prev == prev.DirectPrev() {
		//if prev.Empty() {
		return b
	}

	//return bucketFromListHead(b.ListHead.DirectPrev())
	return bucketFromListHead(prev)

}

func (b *bucket) NextOnLevel() *bucket {

	n := b.LevelHead.Next()
	nn := n.Next()
	if n == nn {
		return b
	}
	return bucketFromLevelHead(n)
	// return bucketFromLevelHead(b.LevelHead.Next())

}

func (b *bucket) PrevOnLevel() *bucket {

	p := b.LevelHead.Prev()
	pp := p.Prev()
	if p == pp {
		return b
	}

	return bucketFromLevelHead(p)

}

func (b *bucket) NextEntry() *entryHMap {

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.DirectNext().Empty() {
		start = start.DirectNext()
	}

	if !start.Empty() {
		return entryHMapFromListHead(start)
	}

	return nil

}

func (b *bucket) PrevEntry() *entryHMap {

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.DirectPrev().Empty() {
		start = start.DirectPrev()
	}

	if !start.Empty() {
		return entryHMapFromListHead(start)
	}

	return nil

}

// func (b *bucket) entry() *entryHMap {
// 	if b.start == nil {
// 		return nil
// 	}
// 	start := b.start
// 	if !start.Empty() {
// 		return entryHMapFromListHead(start)
// 	}
// 	return b.NextEntry()
// }

func (b *bucket) entry(h *Map) (e HMapEntry) {

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.Empty() {
		return h.ItemFn().HmapEntryFromListHead(start)
	}
	return b.NextEntry()

}

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
	if h.modeForBucket == CombineSearch2 {
		return h.searchBybucket(h.searchBucket4Key(k, ignoreBucketEnry))
	} else {
		return h.searchBybucket(h.searchBucket4Key3(k, ignoreBucketEnry))
	}
}

func (h *Map) topLevelBucket(reverse uint64) *bucket {
	i := int(reverse >> (4 * 15))

	if h.topBuckets[i] == nil {
		return h.levelBucket(1).PrevOnLevel().NextOnLevel()
	}
	return h.topBuckets[i]
}

func (h *Map) searchBucket4update(k uint64) *bucket {

	reverseNoMask := bits.Reverse64(k)
	topLevelBucket := h.topLevelBucket(reverseNoMask)

	var upCur *bucket

	for lbCur := topLevelBucket; true; lbCur = lbCur.PrevOnLevel() {
	RETRY:
		if EnableStats {
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
			return upCur
		} else {
			return lbCur
		}

	}
	return nil
}

func (h *Map) searchBucket4Key3(k uint64, ignoreBucketEnry bool) (*bucket, [16]*bucket, uint64, bool) {

	reverseNoMask := bits.Reverse64(k)
	topLevelBucket := h.topLevelBucket(reverseNoMask)
	//gTopLevelBucket := topLevelBucket.PrevOnLevel()

	var upCur *bucket
	//var plbCur *bucket
	levels := [16]*bucket{}

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
			return upCur, levels, reverseNoMask, ignoreBucketEnry
		} else {
			return lbCur, levels, reverseNoMask, ignoreBucketEnry
		}

	}
	return nil, levels, 0, ignoreBucketEnry
}

func (h *Map) searchBucket4Key2(k uint64, ignoreBucketEnry bool) (*bucket, [16]*bucket, uint64, bool) {

	//conf := sharedSearchOpt

	level := 1
	reverseNoMask := bits.Reverse64(k)
	//topLevelBucket := h.levelBucket(level).PrevOnLevel().NextOnLevel()
	topLevelBucket := h.topLevelBucket(reverseNoMask)

	// var reverse uint64
	// _ = reverse

	levels := [16]*bucket{}

	var setLevelCache func(b *bucket)
	if false {
		//if h.modeForBucket == CombineSearch2 {

		levels[level-1] = topLevelBucket //uintptr(unsafe.Pointer(topLevelBucket))
		setLevelCache = func(b *bucket) {
			levels[b.level-1] = b //uintptr(unsafe.Pointer(b))
		}
	} else {
		setLevelCache = func(b *bucket) {}
	}

	var plbCur *bucket
	var upCur *bucket
	for lbCur := topLevelBucket; true; lbCur = lbCur.NextOnLevel() {
	RETRY:
		setLevelCache(lbCur)
		if EnableStats && ignoreBucketEnry {
			h.mu.Lock()
			DebugStats[CntLevelBucket]++
			h.mu.Unlock()
		}
		//reverse = bits.Reverse64(k & toMask(lbCur.level))
		if lbCur.reverse > reverseNoMask && lbCur != lbCur.NextOnLevel() {
			continue
		}
		if lbCur.downLevel == nil {
			goto EACH_ENTRY
		}
		plbCur = lbCur.PrevOnLevel()
		if plbCur.downLevel == nil {
			goto EACH_ENTRY
		}
		upCur = lbCur
		if plbCur != lbCur {
			lbCur = bucketFromLevelHead(plbCur.downLevel)
			goto RETRY
		}
		lbCur = bucketFromLevelHead(lbCur.downLevel)
		goto RETRY

	EACH_ENTRY:
		if upCur != nil && nearUint64(upCur.reverse, lbCur.reverse, reverseNoMask) == upCur.reverse {
			return upCur, levels, reverseNoMask, ignoreBucketEnry
		} else {
			return lbCur, levels, reverseNoMask, ignoreBucketEnry
		}

		//return h.searchBybucket(lbCur, levels, reverseNoMask, ignoreBucketEnry)
	}
	return nil, levels, 0, ignoreBucketEnry
}

func (h *Map) searchBucket4Key(k uint64, ignoreBucketEnry bool) (*bucket, [16]*bucket, uint64, bool) {

	conf := sharedSearchOpt

	level := 1
	reverseNoMask := bits.Reverse64(k)
	//topLevelBucket := h.levelBucket(level).PrevOnLevel().NextOnLevel()
	topLevelBucket := h.topLevelBucket(reverseNoMask)

	// var reverse uint64
	// _ = reverse
	found := false
	var bCur *bucket

	levels := [16]*bucket{}

	var setLevelCache func(b *bucket)
	if h.modeForBucket == CombineSearch2 {

		levels[level-1] = topLevelBucket //uintptr(unsafe.Pointer(topLevelBucket))
		setLevelCache = func(b *bucket) {
			levels[b.level-1] = b //uintptr(unsafe.Pointer(b))
		}
	} else {
		setLevelCache = func(b *bucket) {}
	}

	for lbCur := topLevelBucket; true; lbCur = lbCur.NextOnLevel() {
	RETRY:
		setLevelCache(lbCur)
		if EnableStats && ignoreBucketEnry {
			h.mu.Lock()
			DebugStats[CntLevelBucket]++
			h.mu.Unlock()
		}
		//reverse = bits.Reverse64(k & toMask(lbCur.level))

		if lbCur.reverse > reverseNoMask && lbCur != lbCur.NextOnLevel() {
			continue
		}
		var plbCur *bucket //:= lbCur.PrevOnLevel()
		var maxReverse uint64
		for plbCur = lbCur; plbCur.PrevOnLevel() != plbCur; plbCur = plbCur.PrevOnLevel() {
			if plbCur.reverse < reverseNoMask {
				continue
			}
			break
		}
		if plbCur == nil {
			plbCur = lbCur
		}
		if plbCur.reverse < reverseNoMask { // && plbCur.PrevOnLevel() == plbCur {

			if plbCur.level == 16 {
				goto EACH_ENTRY
			}
			if plbCur.level == 15 {
				goto EACH_ENTRY
			}
			if !h.isEmptyBylevel(plbCur.level + 1) {
				lbCur = h.levelBucket(plbCur.level + 1).PrevOnLevel().NextOnLevel()
				continue
			}

			goto EACH_ENTRY
		}

		if plbCur.reverse < reverseNoMask {
			conf.e = errors.New("searchKey: not invalid destinatioon")
			return nil, levels, 0, ignoreBucketEnry
		}
		found = false
		maxReverse = (plbCur.reverse - reverseNoMask)
		if reverseNoMask-maxReverse > 0 {
			maxReverse = reverseNoMask - maxReverse
		} else {
			maxReverse = reverseNoMask
		}
		for bCur = plbCur; bCur.reverse > maxReverse; bCur = bCur.nextAsB() {
			if EnableStats && ignoreBucketEnry {
				h.mu.Lock()
				DebugStats[CntSearchBucket]++
				h.mu.Unlock()
			}
			if bCur.level == level+1 {
				level++
				found = true
				lbCur = bCur
				break
			}
		}
		setLevelCache(bCur)
		setLevelCache(lbCur)
		if !found {
			//lbCur = lbCur.prevAsB()
			if bCur.reverse > reverseNoMask {
				lbCur = bCur
			} else {
				lbCur = bCur.prevAsB()
			}
			setLevelCache(lbCur)
			goto EACH_ENTRY
		}

		if !h.isEmptyBylevel(level) {
			found = false
			goto RETRY
		}

	EACH_ENTRY:
		return lbCur, levels, reverseNoMask, ignoreBucketEnry
		//return h.searchBybucket(lbCur, levels, reverseNoMask, ignoreBucketEnry)
	}
	return nil, levels, 0, ignoreBucketEnry
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

func (h *Map) searchBybucket(lbCur *bucket, levels [16]*bucket, reverseNoMask uint64, ignoreBucketEnry bool) HMapEntry {
	if lbCur == nil {
		return nil
	}

	lbNext := lbCur

	if h.modeForBucket == CombineSearch2 && lbCur.reverse > reverseNoMask {
		lbNext = lbCur.NextOnLevel()
		lbNext = nearBucketFromCache(levels, lbNext, reverseNoMask)
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

func (h *Map) ActiveLevels() (result []int) {

	for i := range h.levelCache {
		if !h.isEmptyBylevel(i + 1) {
			result = append(result, i+1)
		}
	}
	return
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
