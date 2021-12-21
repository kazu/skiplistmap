// Copyright 2019-2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package loncha/list_head is like a kernel's LIST_HEAD
// list_head is used by loncha/gen/containers_list
package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/kazu/elist_head"
)

type SampleItem struct {
	K string
	V atomic.Value
	MapHead
}

var sampleItem MapItem = &SampleItem{}

//var EmptySampleHMapEntry SampleItem = SampleItem{}
var EmptySampleHMapEntry *SampleItem = (*SampleItem)(unsafe.Pointer(uintptr(0)))

const SampleItemOffsetOf = unsafe.Offsetof(EmptySampleHMapEntry.ListHead)
const SampleItemSize = unsafe.Sizeof(*EmptySampleHMapEntry)

func SampleItemFromListHead(head *elist_head.ListHead) *SampleItem {
	return (*SampleItem)(ElementOf(unsafe.Pointer(head), SampleItemOffsetOf))
}

func (s *SampleItem) Offset() uintptr {
	return SampleItemOffsetOf
}

func (s *SampleItem) PtrMapeHead() *MapHead {
	return &(s.MapHead)
}

func (s *SampleItem) hmapEntryFromListHead(lhead *elist_head.ListHead) *SampleItem {
	return SampleItemFromListHead(lhead)
}

func (s *SampleItem) HmapEntryFromListHead(lhead *elist_head.ListHead) HMapEntry {
	return s.hmapEntryFromListHead(lhead)
}

func (s *SampleItem) Key() interface{} {
	return s.K
}

func (s *SampleItem) Value() interface{} {
	return s.V.Load()
}

func (s *SampleItem) SetValue(v interface{}) bool {
	s.V.Store(v)
	return true
}

func (s *SampleItem) Setup() {
	s.reverse, s.conflict = KeyToHash(s.Key())

}

func (s *SampleItem) Next() HMapEntry {
	return s.hmapEntryFromListHead(s.PtrListHead().DirectNext())
}
func (s *SampleItem) Prev() HMapEntry {
	return s.hmapEntryFromListHead(s.PtrListHead().DirectPrev())
}

func (s *SampleItem) PtrMapHead() *MapHead {
	return &s.MapHead
}

func (s *SampleItem) Delete() {
	s.state |= mapIsDummy
}

func (s *SampleItem) KeyHash() (uint64, uint64) {
	return MemHashString(s.K), xxhash.Sum64String(s.K)
}

func NewSampleItem(key string, value interface{}) (item *SampleItem) {
	item = &SampleItem{K: key}
	item.SetValue(value)
	return
}
