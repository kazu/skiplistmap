// Copyright 2019-2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package loncha/list_head is like a kernel's LIST_HEAD
// list_head is used by loncha/gen/containers_list
package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

type SampleItem[Key, Value any] struct {
	K Key
	V atomic.Value
	MapHead
}

//var sampleItem MapItem = &SampleItem{}

func sampleItem[Key, Value any]() MapItem[Key, Value] {
	return &SampleItem[Key, Value]{}
}

func EmptySampleHMapEntry[K, V any]() *SampleItem[K, V] {

	return (*SampleItem[K, V])(unsafe.Pointer(uintptr(0)))
}

func SampleItemOffsetOf[K, V any]() uintptr {

	return unsafe.Offsetof(EmptySampleHMapEntry[K, V]().ListHead)

}

//const SampleItemSize = unsafe.Sizeof(*EmptySampleHMapEntry)

func SampleItemSize[K, V any]() uintptr {
	return unsafe.Sizeof(*EmptySampleHMapEntry[K, V]())
}

func SampleItemFromListHead[K, V any](head *elist_head.ListHead) *SampleItem[K, V] {
	return (*SampleItem[K, V])(ElementOf(unsafe.Pointer(head), SampleItemOffsetOf[K, V]()))
}

func (s *SampleItem[K, V]) Offset() uintptr {
	return SampleItemOffsetOf[K, V]()
}

func (s *SampleItem[K, V]) PtrMapeHead() *MapHead {
	return &(s.MapHead)
}

func (s *SampleItem[K, V]) hmapEntryFromListHead(lhead *elist_head.ListHead) *SampleItem[K, V] {
	return SampleItemFromListHead[K, V](lhead)
}

func (s *SampleItem[K, V]) HmapEntryFromListHead(lhead *elist_head.ListHead) HMapEntry {
	return s.hmapEntryFromListHead(lhead)
}

func (s *SampleItem[K, V]) Key() K {
	return s.K
}

func (s *SampleItem[K, V]) Value() V {
	return s.V.Load().(V)
}

func (s *SampleItem[K, V]) SetValue(v V) bool {
	// if v == nil {
	// 	return false
	// }
	s.V.Store(v)
	return true
}

func (s *SampleItem[K, V]) Setup() {
	s.reverse, s.conflict = KeyToHash(s.Key())

}

func (s *SampleItem[K, V]) Next() HMapEntry {
	return s.hmapEntryFromListHead(s.PtrListHead().DirectNext())
}
func (s *SampleItem[K, V]) Prev() HMapEntry {
	return s.hmapEntryFromListHead(s.PtrListHead().DirectPrev())
}

func (s *SampleItem[K, V]) PtrMapHead() *MapHead {
	return &s.MapHead
}

func (s *SampleItem[K, V]) Delete() {
	s.state |= mapIsDeleted
}

func (s *SampleItem[K, V]) KeyHash() (uint64, uint64) {
	//return MemHashString(s.K), xxhash.Sum64String(s.K)
	return KeyToHash(s.K)
}

func NewSampleItem[Key, Value any](key Key, value Value) (item *SampleItem[Key, Value]) {
	item = &SampleItem[Key, Value]{K: key}
	item.SetValue(value)
	return
}
