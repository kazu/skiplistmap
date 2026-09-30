// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

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

func halfUint64(os uint64, oe uint64) (hafl uint64) {

	s, e := os, oe
	if s > e {
		s, e = e, s
	}

	diff := e - s
	if diff == 0 {
		return s + diff/2
	}
	return s + diff/2

}

//go:nocheckptr
func ElementOf(head unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(head) - offset)
}

func PoolCap(len int) int {
	min := minCapItem()

	if len < min {
		return min
	}

	// the capacity doubles: an expand moves every item, so that a fixed
	// step would move them again every few inserts
	for i := 0; i < 60; i++ {
		if (len >> i) == 0 {
			return intPow(2, i)
		}
	}
	return 512
}

func calcCap(len int) int {

	if len > 1024 {
		return len + len/4
	}

	for i := 0; i < 60; i++ {
		if (len >> i) == 0 {
			return intPow(2, i)
		}

	}
	return 512

}

func intPow(a, b int) (r int) {
	r = 1
	for i := 0; i < b; i++ {
		r *= a
	}
	return
}

func maxInts(ints ...int) (max int) {
	for i := range ints {
		if i == 0 {
			max = ints[i]
			continue
		}
		if max < ints[i] {
			max = ints[i]
		}
	}
	return
}

func NilMapEntry() HMapEntry {
	return (*entryHMap)(nil)
}

// insertInOrder links center just before right in one attempt, only if the
// entry that is before right when center is linked does not come after center.
// It returns an error without linking center otherwise; the caller finds the
// position again.
func insertInOrder(right, center *elist_head.ListHead, entry HMapEntry) error {

	if err := checkLinkBefore(right, center); err != nil {
		return err
	}
	centermHead := mapheadFromLListHead(center)
	return right.TryInsertBefore(center, func(left *elist_head.ListHead) bool {
		return canLinkAfter(mapheadFromLListHead(left), centermHead, entry)
	})
}

// checkLinkBefore returns an error unless center is not linked and its key
// does not come after the key of right.
func checkLinkBefore(right, center *elist_head.ListHead) error {

	centermHead := mapheadFromLListHead(center)
	rightmHead := mapheadFromLListHead(right)
	if !center.Empty() && !center.IsSingle() {
		return NewError(EIItemInvalidAdd, "invalid left state ", nil)
	}

	if rightmHead.reverse < centermHead.reverse {
		return NewError(EIItemInvalidAdd, "invalid insert order", nil)
	}
	return nil
}

// canLinkAfter reports whether center may be linked just after left: left
// does not come after center, and left is not a live entry of the key of
// center, which another store may have linked since center was looked up.
func canLinkAfter(left, center *MapHead, entry HMapEntry) bool {
	return left.Empty() || (left.reverse <= center.reverse && linkedSameKey(left, center, entry) == nil)
}

// linkedSameKey returns the live entry of the key of center among left and
// the entries before left with the reverse of center, or nil. Entries of one
// reverse lie in any order, including distinct keys with equal hash pairs.
func linkedSameKey(left, center *MapHead, entry HMapEntry) *MapHead {
	for m := left; !m.Empty() && m.reverse == center.reverse; m = mapheadFromLListHead(m.PtrListHead().DirectPrev()) {
		if sameKeyLinked(m, center) {
			if item, ok := entry.(MapItem); ok {
				other := entry.HmapEntryFromListHead(m.PtrListHead()).(MapItem)
				if !equalItemKey(other, item.Key()) {
					continue
				}
			}
			return m
		}
	}
	return nil
}

// sameKeyLinked reports whether left is an entry of the key of center that
// is not deleted.
func sameKeyLinked(left, center *MapHead) bool {
	return !left.Empty() && !left.IsIgnored() && left != center &&
		left.reverse == center.reverse &&
		atomic.LoadUint64(&left.conflict) == atomic.LoadUint64(&center.conflict) &&
		!left.PtrListHead().IsMarked()
}
