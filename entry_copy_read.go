package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// Nonembedded entries publish immutable copies, including entries allocated
// by the map's pool. Searches follow replacements and skip bucket dummies.
// Distinct array lengths specialize the search at compile time: ordinary
// lookups have no per-node dummy tracking branch or store.
type dummyTrace interface{ ~[0]byte | ~[1]byte }
type noDummyTrace [0]byte
type saveDummyTrace [1]byte

func (h *Map[K, V]) searchCopyEntry[T dummyTrace](b *bucket[K, V], start *MapHead, reverse uint64, ignoreDummy bool, dummy **MapHead) HMapEntry[K, V] {
	var trace T
	if start == nil {
		return nil
	}
	stepAt("copy.search", unsafe.Pointer(start.PtrListHead()), nil)
	forward := b.reverse < reverse
SEARCH:
	for {
		cur := start
		if len(trace) != 0 {
			*dummy = nil
		}
	WALK:
		for cur != nil {
			if cur.PtrListHead().IsMarked() {
				break
			}
			if stepEnabled {
				stepAt("search.unmarked", unsafe.Pointer(cur.PtrListHead()), nil)
			}
			if EnableStats && ignoreDummy {
				h.mu.Lock()
				if forward {
					DebugStats[CntReverseSearch]++
				} else {
					DebugStats[CntSearchEntry]++
				}
				h.mu.Unlock()
			}
			if len(trace) != 0 && cur.IsDummy() {
				*dummy = cur
			}
			r := atomic.LoadUint64(&cur.reverse)
			if r == reverse {
				if !cur.IsDummy() && (!ignoreDummy || !cur.IsIgnored()) {
					match := cur
					// Copies are inserted before their old entry. A backward
					// search must reach the start of this hash group first;
					// key/conflict matching then sees newer copies first.
					for head := cur.PtrListHead(); !forward; {
						prev := head.DirectPrev()
						if prev == head || prev.Empty() {
							break
						}
						if prev.IsMarked() {
							break WALK
						}
						mh := mapheadFromLListHead(prev)
						if atomic.LoadUint64(&mh.reverse) != reverse {
							break
						}
						if len(trace) != 0 && mh.IsDummy() {
							*dummy = mh
						}
						if !mh.IsDummy() && (!ignoreDummy || !mh.IsIgnored()) {
							match = mh
						}
						head = prev
					}
					return entryHMapFromListHead[K, V](match.PtrListHead())
				}
				// A replacement may have retired this cursor after the
				// first mark check. Do not skip it and report a missing key.
				if cur.PtrListHead().IsMarked() {
					break
				}
			} else if forward && r > reverse || !forward && r < reverse {
				goto NOT_FOUND
			}
			head := cur.PtrListHead()
			var next *elist_head.ListHead
			if forward {
				next = head.DirectNext()
			} else {
				next = head.DirectPrev()
			}
			if next == head || next.Empty() {
				goto NOT_FOUND
			}
			cur = mapheadFromLListHead(next)
		}
		if cur == nil {
			goto NOT_FOUND
		}
		stepAt("search.marked", unsafe.Pointer(cur.PtrListHead()), nil)
		runtime.Gosched()
		e := b.entry(h)
		if e == nil {
			goto NOT_FOUND
		}
		start = e
	}
NOT_FOUND:
	if !forward && b.reverse == reverse {
		forward = true
		goto SEARCH
	}
	return nil

}

func (h *Map[K, V]) matchCopyEntry(entry *entryHMap[K, V], reverse, conflict uint64, key K, byKey bool) (HMapEntry[K, V], bool) {
	return h.matchCopyEntryWithDummy(entry, reverse, conflict, key, byKey, nil)
}

func (h *Map[K, V]) matchCopyEntryWithDummy(entry *entryHMap[K, V], reverse, conflict uint64, key K, byKey bool, dummy **MapHead) (HMapEntry[K, V], bool) {
	stepAt("copy.match", unsafe.Pointer(entry.PtrListHead()), nil)
	matches := func(e *entryHMap[K, V]) bool {
		return !e.IsIgnored() && atomic.LoadUint64(&e.reverse) == reverse &&
			atomic.LoadUint64(&e.conflict) == conflict && linkedEntry(e.PtrListHead()) &&
			(!byKey || equalKeys[K, V](e.key, key))
	}
	if entry.ListHead.IsMarked() {
		return nil, true
	}
	if matches(entry) {
		if entry.ListHead.IsMarked() {
			return nil, true
		}
		return entry, false
	}
	for _, forward := range [...]bool{true, false} {
		cur := entry.PtrListHead()
		for {
			if forward {
				cur = cur.DirectNext()
			} else {
				cur = cur.DirectPrev()
			}
			if cur == h.head || cur == h.tail {
				break
			}
			if cur.IsMarked() {
				return nil, true
			}
			mh := mapheadFromLListHead(cur)
			if mh.IsDummy() {
				if dummy != nil {
					*dummy = mh
				}
				if atomic.LoadUint64(&mh.reverse) != reverse {
					break
				}
				continue
			}
			e := entryHMapFromListHead[K, V](cur)
			if !linkedEntry(cur) {
				return nil, true
			}
			if atomic.LoadUint64(&e.reverse) != reverse {
				break
			}
			if matches(e) {
				if cur.IsMarked() {
					return nil, true
				}
				return e, false
			}
		}
	}
	return nil, entry.ListHead.IsMarked()
}

func (h *Map[K, V]) rangeCopyEntries(first *entryHMap[K, V], f func(MapItem[K, V]) bool) {
	stepAt("copy.range", unsafe.Pointer(first.PtrListHead()), nil)
	for cur := first.PtrListHead(); !cur.Empty(); cur = cur.DirectNext() {
		for cur.IsMarked() {
			cur = elist_head.PrevNoM(cur).DirectNext()
			runtime.Gosched()
		}
		if cur.Empty() {
			return
		}
		mh := mapheadFromLListHead(cur)
		if mh.IsIgnored() {
			continue
		}
		e := entryHMapFromListHead[K, V](cur)
		if e.IsIgnored() {
			continue
		}
		if !f(e) {
			return
		}
	}
}
