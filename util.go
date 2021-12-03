// Package skitlistmap ... concurrent akiplist map implementatin
// Copyright 2201 Kazuhisa TAKEI<xtakei@rytr.jp>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package skiplistmap

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
