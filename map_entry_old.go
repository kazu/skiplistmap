//go:build !go1.18
// +build !go1.18

// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
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

func entryHMapFromPlistHead(head unsafe.Pointer) *entryHMap {
	return (*entryHMap)(ElementOf(head, entryHMapOffset))
}

func entryHMapFromListHead(head *elist_head.ListHead) *entryHMap {
	return entryHMapFromPlistHead(unsafe.Pointer(head))
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
