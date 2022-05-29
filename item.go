//go:build go1.18
// +build go1.18

// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
	"sync"

	"github.com/kazu/elist_head"
)

type HMapEntry interface {
	Offset() uintptr
	PtrMapHead() *MapHead
	PtrListHead() *elist_head.ListHead
	HmapEntryFromListHead(*elist_head.ListHead) HMapEntry
	Next() HMapEntry
	Prev() HMapEntry
}
type MapItem[K any, V any] interface {
	Key() K   // require order for HMap
	Value() V // require order for HMap
	SetValue(V) bool
	KeyHash() (uint64, uint64)
	Delete()

	HMapEntry
}

type CondOfFinder[K any, V any] func(ehead *entryHMap[K, V]) bool

func CondOfFind[K any, V any](reverse uint64, l sync.Locker) CondOfFinder[K, V] {

	return func(ehead *entryHMap[K, V]) bool {

		if EnableStats {
			l.Lock()
			DebugStats[CntSearchEntry]++
			l.Unlock()
		}
		return reverse <= ehead.reverse
	}

}

type entryBuffer[K any, V any] struct {
	entries []entryHMap[K, V]

	elist_head.ListHead
}

func (ebuf *entryBuffer[K, V]) Len() int {
	return len(ebuf.entries)
}

func (ebuf *entryBuffer[K, V]) init(cap int) {

	ebuf.entries = make([]entryHMap[K, V], 1, cap)

}

func (ebuf *entryBuffer[K, V]) getEntryFromPool(idx int) *entryHMap[K, V] {

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
