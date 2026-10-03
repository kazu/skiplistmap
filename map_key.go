package skiplistmap

import (
	"github.com/kazu/elist_head"
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"
)

func equalKeys[K Key[K], V any](a, b K) bool { return a.Equal(b) }
func equalItemKey[K Key[K], V any](item *embeddedEntry[K, V], key K) bool {
	return item.Key().Equal(key)
}

func (h *Map[K, V]) endValue(last bool) (V, bool) {
	defer runtime.KeepAlive(h)
	var zero V
	var e *embeddedEntry[K, V]
	if last {
		e = h.last()
	} else {
		e = h.first()
	}
	if e == nil {
		return zero, false
	}
	if stepEnabled {
		stepAt("end.selected", unsafe.Pointer(e.PtrListHead()), nil)
	}
	state := atomic.LoadUint64((*uint64)(&e.state)) &^ uint64(mapTransientState)
	value, ok := h.readEntryValue(e, atomic.LoadUint64(&e.reverse))
	if !ok {
		return zero, false
	}
	if h.isEmbededItemInBucket {
		// The selected slot may already have a different key. Verify its
		// position again, with the payload generation spanning that walk.
		var current *embeddedEntry[K, V]
		if last {
			current = h.last()
		} else {
			current = h.first()
		}
		if current != e || atomic.LoadUint64((*uint64)(&e.state))&^uint64(mapTransientState) != state {
			return zero, false
		}
	}
	return value, true
}

func (h *Map[K, V]) readEntryValue(e *embeddedEntry[K, V], reverse uint64) (V, bool) {
	conflict := atomic.LoadUint64(&e.conflict)
	if stepEnabled {
		stepAt("get.beforeValue", unsafe.Pointer(e.PtrListHead()), nil)
	}
	var key K
	return readMatchingEntry(e, reverse, conflict, key, false, true, h.isEmbededItemInBucket)
}

func (h *Map[K, V]) getValueMatching(hash, conflict uint64, key K, byKey bool) (V, bool) {
	e := h.searchItem(hash)
	for e != nil {
		if stepEnabled {
			stepAt("get.beforeValue", unsafe.Pointer(e.PtrListHead()), nil)
		}
		if value, ok := readMatchingEntry[K, V](e, bits.Reverse64(hash), conflict, key, byKey, true, h.isEmbededItemInBucket); ok {
			return value, true
		}
		item, found := h.getItemMatching(hash, conflict, key, byKey)
		if !found {
			var zero V
			return zero, false
		}
		e = item
	}
	var zero V
	return zero, false
}

func readMatchingEntry[K Key[K], V any](e *embeddedEntry[K, V], reverse, conflict uint64, key K, byKey, withValue, reusable bool) (V, bool) {
	var zero V
	if e == nil {
		return zero, false
	}
	stored, value, state := e.loadKeyValue(withValue)
	if state&(mapIsDummy|mapIsDeleted) != 0 || (byKey && !stored.Equal(key)) {
		return zero, false
	}
	if !matchesHash(e.PtrMapHead(), reverse, conflict) {
		return zero, false
	}
	if reusable && mapState(atomic.LoadUint64((*uint64)(&e.state)))&^mapTransientState != state&^mapTransientState {
		return zero, false
	}
	return value, true
}

func matchesLiveHash(mh *MapHead, reverse, conflict uint64) bool {
	return !mh.IsIgnored() && matchesHash(mh, reverse, conflict)
}

func matchesHash(mh *MapHead, reverse, conflict uint64) bool {
	return atomic.LoadUint64(&mh.reverse) == reverse && atomic.LoadUint64(&mh.conflict) == conflict &&
		linkedEntry(mh.PtrListHead())
}

func linkedEntry(head *elist_head.ListHead) bool {
	return head.DirectNext() != head && head.DirectPrev() != head
}
