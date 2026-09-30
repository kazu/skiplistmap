package skiplistmap

import (
	"github.com/kazu/elist_head"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

func equalKeys[K Key[K], V any](a, b K) bool                      { return a.Equal(b) }
func equalItemKey[K Key[K], V any](item *Entry[K, V], key K) bool { return item.Key().Equal(key) }

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

func readMatchingEntry[K Key[K], V any](e *Entry[K, V], reverse, conflict uint64, key K, byKey, withValue, reusable bool) (V, bool) {
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
	if reusable && mapState(atomic.LoadUint64((*uint64)(&e.state)))&^mapIsBusy != state&^mapIsBusy {
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
