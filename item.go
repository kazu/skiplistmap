// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"sync"
	"unsafe"

	"github.com/kazu/elist_head"
)

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
	emptyEntryHMap *entryHMap = nil
	emptyBucket    *bucket    = nil
	EmptyEntryHMap *entryHMap = nil
)

func entryHMapFromListHead(head *elist_head.ListHead) *entryHMap {
	return (*entryHMap)(elist_head.ElementOf(emptyEntryHMap, head))
}

func (e *entryHMap) entryHMapromListHead(lhead *elist_head.ListHead) *entryHMap {
	return entryHMapFromListHead(lhead)
}

func (s *entryHMap) HmapEntryFromListHead(lhead *elist_head.ListHead) HMapEntry {
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

const entryHMapOffset = unsafe.Offsetof(EmptyEntryHMap.ListHead)

func (s *entryHMap) Offset() uintptr {
	return entryHMapOffset
}

func (s *entryHMap) Delete() {
	s.key = nil
	s.MapHead.state |= mapIsDeleted
}

func (s *entryHMap) KeyHash() (uint64, uint64) {
	return KeyToHash(s.key)
}

type HMapEntry interface {
	Offset() uintptr
	PtrMapHead() *MapHead
	PtrListHead() *elist_head.ListHead
	HmapEntryFromListHead(*elist_head.ListHead) HMapEntry
	Next() HMapEntry
	Prev() HMapEntry
}
type MapItem interface {
	Key() interface{}   // require order for HMap
	Value() interface{} // require order for HMap
	SetValue(interface{}) bool
	KeyHash() (uint64, uint64)
	Delete()

	HMapEntry
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

type entryBuffer struct {
	entries []entryHMap

	elist_head.ListHead
}

func (ebuf *entryBuffer) Len() int {
	return len(ebuf.entries)
}

func (ebuf *entryBuffer) init(cap int) {

	ebuf.entries = make([]entryHMap, 1, cap)

}

func (ebuf *entryBuffer) getEntryFromPool(idx int) *entryHMap {

	// if len(ebuf.entries) == 1 {
	// 	ebuf.entries = ebuf.entries[:2]
	// 	e := &ebuf.entries[1]
	// 	e.reverse = reverse
	// 	return e
	// }

	// lastE := ebuf.entries[len(ebuf.entries)-1]
	// if lastE.reverse <

	if cap(ebuf.entries) <= idx {
		// FIXME: goto nextbuffer
	}

	if len(ebuf.entries) == idx {
		ebuf.entries = ebuf.entries[:idx+1]
	}

	ebuf.entries[idx].MapHead = ebuf.entries[idx-1].MapHead
	e := &ebuf.entries[idx]
	e.reverse = 0
	return e

}
