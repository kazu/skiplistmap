package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// These functions are exclusively for caller-owned entryHMap copies. Dispatch
// uses the concrete entry type already obtained by the ordinary lookup. The
// SampleItem and custom MapItem paths retain their original search predicates.
func (h *Map) searchCopyEntry(b *bucket, start *entryHMap, reverse uint64, ignoreDummy bool) HMapEntry {
	stepAt("copy.search", unsafe.Pointer(start.PtrListHead()), nil)
	forward := b.reverse < reverse
	for {
		cur := start.PtrMapHead()
		for cur != nil {
			if cur.PtrListHead().IsMarked() {
				break
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
			r := atomic.LoadUint64(&cur.reverse)
			if r == reverse {
				if !ignoreDummy || !cur.IsIgnored() {
					return entryHMapFromListHead(cur.PtrListHead())
				}
				// A replacement may have retired this cursor after the
				// first mark check. Do not skip it and report a missing key.
				if cur.PtrListHead().IsMarked() {
					break
				}
			} else if forward && r > reverse || !forward && r < reverse {
				return nil
			}
			head := cur.PtrListHead()
			var next *elist_head.ListHead
			if forward {
				next = head.DirectNext()
			} else {
				next = head.DirectPrev()
			}
			if next == head || next.Empty() {
				return nil
			}
			cur = mapheadFromLListHead(next)
		}
		if cur == nil {
			return nil
		}
		stepAt("search.marked", unsafe.Pointer(cur.PtrListHead()), nil)
		runtime.Gosched()
		e := b.entry(h)
		if e == nil {
			return nil
		}
		var ok bool
		start, ok = e.(*entryHMap)
		if !ok {
			// The map changed its item implementation; use that path.
			return h._searchBybucket(b, reverse, ignoreDummy)
		}
	}
}

func (h *Map) matchCopyEntry(entry *entryHMap, reverse, conflict uint64, key interface{}, byKey bool) (HMapEntry, bool) {
	stepAt("copy.match", unsafe.Pointer(entry.PtrListHead()), nil)
	matches := func(e *entryHMap) bool {
		return !e.IsIgnored() && atomic.LoadUint64(&e.reverse) == reverse &&
			atomic.LoadUint64(&e.conflict) == conflict && linkedEntry(e.PtrListHead()) &&
			(!byKey || equalKeys(e.key, key))
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
			e := entryHMapFromListHead(cur)
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

func (h *Map) rangeCopyEntries(first *entryHMap, f func(MapItem) bool) {
	stepAt("copy.range", unsafe.Pointer(first.PtrListHead()), nil)
	for cur := first.PtrListHead(); !cur.Empty(); cur = cur.DirectNext() {
		for cur.IsMarked() {
			cur = elist_head.PrevNoM(cur).DirectNext()
			runtime.Gosched()
		}
		if cur.Empty() {
			return
		}
		e := entryHMapFromListHead(cur)
		if e.IsIgnored() {
			continue
		}
		if !f(e) {
			return
		}
	}
}
