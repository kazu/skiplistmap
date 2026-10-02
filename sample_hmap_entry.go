package skiplistmap

import (
	"github.com/kazu/elist_head"
	"sync/atomic"
	"unsafe"
)

func NewSampleItem[K Key[K], V any](key K, value V) *Entry[K, V] {
	return NewEntry(key, value)
}
func EmptySampleHMapEntry[K Key[K], V any]() *Entry[K, V] { return nil }
func SampleItemOffsetOf[K Key[K], V any]() uintptr        { return entryHMapOffset[K, V]() }
func SampleItemSize[K Key[K], V any]() uintptr            { var e Entry[K, V]; return unsafe.Sizeof(e) }
func SampleItemFromListHead[K Key[K], V any](head *elist_head.ListHead) *Entry[K, V] {
	return entryHMapFromListHead[K, V](head)
}
func (e *Entry[K, V]) HmapEntryFromListHead(head *elist_head.ListHead) *Entry[K, V] {
	return entryHMapFromListHead[K, V](head)
}
func (e *Entry[K, V]) Next() *Entry[K, V] {
	for head := e.PtrListHead().DirectNext(); !head.Empty(); head = head.DirectNext() {
		if !mapheadFromLListHead(head).IsDummy() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}
func (e *Entry[K, V]) Prev() *Entry[K, V] {
	for head := e.PtrListHead().DirectPrev(); !head.Empty(); head = head.DirectPrev() {
		if !mapheadFromLListHead(head).IsDummy() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}

// SetValue initializes an unlinked Entry. Change linked entries through Map.Set.
func (e *Entry[K, V]) SetValue(value V) bool {
	if !e.PtrListHead().IsSingle() {
		return false
	}
	e.storeTypedKeyValue(e.Key(), value)
	return true
}
func (e *Entry[K, V]) Delete() {
	atomic.OrUint64((*uint64)(&e.state), uint64(mapIsDeleted|mapIsRetired))
}
func (e *Entry[K, V]) Setup() { e.reverse, e.conflict = e.KeyHash() }

func (e *Entry[K, V]) copyFrom(src *Entry[K, V]) {
	key, value, state := src.loadTypedKeyValue()
	if stepEnabled {
		stepAt("item.copy.read", unsafe.Pointer(e.PtrListHead()), unsafe.Pointer(src.PtrListHead()))
	}
	if src.waitPayload() {
		e.initializePayload(key, value, src.root, src.reusable)
	}
	atomic.OrUint64((*uint64)(&e.state), uint64(state&(mapIsDummy|mapIsDeleted|mapIsPoolItem)))
	e.conflict = atomic.LoadUint64(&src.conflict)
	e.reverse = atomic.LoadUint64(&src.reverse)
}
