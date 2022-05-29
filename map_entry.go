//go:build go1.18
// +build go1.18

// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"unsafe"

	"github.com/kazu/elist_head"
)

type entryHMap[K any, V any] struct {
	key   K
	value V

	MapHead
}

func NewEntryMap[K any, V any](key K, value V) *entryHMap[K, V] {
	return &entryHMap[K, V]{
		key:   key,
		value: value,
	}
}

// var (
// 	emptyEntryHMap *entryHMap[K any, V any] = nil
// 	emptyBucket    *bucket    = nil
// 	EmptyEntryHMap *entryHMap[K any, v any] = nil
// )

func emptyEntryHMap[K any, V any]() *entryHMap[K, V] {
	return nil
}
func emptyBucket[K any, V any]() *entryHMap[K, V] {
	return nil
}

func EmptyEntryHMap[K any, V any]() *entryHMap[K, V] {
	return nil
}

func entryHMapFromPlistHead[K any, V any](head unsafe.Pointer) *entryHMap[K, V] {
	return (*entryHMap[K, V])(ElementOf(head, entryHMapOffset[K, V]()))
}

func entryHMapFromListHead[K, V any](head *elist_head.ListHead) *entryHMap[K, V] {
	return entryHMapFromPlistHead[K, V](unsafe.Pointer(head))
}

func (e *entryHMap[K, V]) entryHMapromListHead(lhead *elist_head.ListHead) *entryHMap[K, V] {
	return entryHMapFromListHead[K, V](lhead)
}

func (s *entryHMap[K, V]) HmapEntryFromListHead(lhead *elist_head.ListHead) HMapEntry {
	return s.entryHMapromListHead(lhead)
}

func (s *entryHMap[K, V]) Key() K {
	return s.key
}

func (s *entryHMap[K, V]) Value() V {
	return s.value
}

func (s *entryHMap[K, V]) SetValue(v V) bool {
	s.value = v
	return true
}

func (s *entryHMap[K, V]) Next() HMapEntry {
	return s.entryHMapromListHead(s.PtrListHead().DirectNext())
}
func (s *entryHMap[K, V]) Prev() HMapEntry {
	return s.entryHMapromListHead(s.PtrListHead().DirectPrev())
}

func (s *entryHMap[K, V]) PtrMapHead() *MapHead {
	return &s.MapHead
}

func (s *entryHMap[K, V]) Offset() uintptr {
	//return entryHMapOffset
	return unsafe.Offsetof((&entryHMap[K, V]{}).ListHead)
}

func (s *entryHMap[K, V]) Delete() {
	//FIXME: check nulable ?
	//s.key = nil
	s.MapHead.state |= mapIsDeleted
}

func (s *entryHMap[K, V]) KeyHash() (uint64, uint64) {
	return KeyToHash(s.key)
}
func entryHMapOffset[K any, V any]() uintptr {

	return unsafe.Offsetof((&entryHMap[K, V]{}).ListHead)

}
