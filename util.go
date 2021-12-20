// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

import (
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

func ElementOf(head unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(head) - offset)
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

func inserBeforeWithCheck(right *elist_head.ListHead, center *elist_head.ListHead) (*elist_head.ListHead, error) {

	centermHead := mapheadFromLListHead(center)
	rightmHead := mapheadFromLListHead(right)
	leftmHead := mapheadFromLListHead(right.Prev())
	if !center.Empty() && !center.IsSingle() {
		return nil, NewError(EIItemInvalidAdd, "invalid left state ", nil)
	}

	if rightmHead.reverse < centermHead.reverse {
		return nil, NewError(EIItemInvalidAdd, "invalid insert order", nil)
	}
	if !leftmHead.Empty() && centermHead.reverse < leftmHead.reverse {
		return nil, NewError(EIItemInvalidAdd, "invalid insert order", nil)
	}

	return right.InsertBefore(center)
}
