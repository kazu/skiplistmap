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

// SampleItemFromListHead returns the external Entry, or nil for an internal embedded slot.
func SampleItemFromListHead[K Key[K], V any](head *elist_head.ListHead) *Entry[K, V] {
	return entryHMapFromListHead[K, V](head).viewEntry()
}

// HmapEntryFromListHead returns the external Entry, or nil for an internal embedded slot.
func (e *Entry[K, V]) HmapEntryFromListHead(head *elist_head.ListHead) *Entry[K, V] {
	return entryHMapFromListHead[K, V](head).viewEntry()
}
func (e *embeddedEntry[K, V]) Next() *embeddedEntry[K, V] {
	for head := e.PtrListHead().DirectNext(); !head.Empty(); head = head.DirectNext() {
		if !mapheadFromLListHead(head).IsDummy() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}
func (e *embeddedEntry[K, V]) Prev() *embeddedEntry[K, V] {
	for head := e.PtrListHead().DirectPrev(); !head.Empty(); head = head.DirectPrev() {
		if !mapheadFromLListHead(head).IsDummy() {
			return entryHMapFromListHead[K, V](head)
		}
	}
	return nil
}

// SetValue initializes an unlinked Entry. Change linked entries through Map.Set.
func (e *embeddedEntry[K, V]) SetValue(value V) bool {
	if !e.PtrListHead().IsSingle() {
		return false
	}
	e.storeTypedKeyValue(e.Key(), value)
	return true
}
func (e *embeddedEntry[K, V]) Delete() {
	atomic.OrUint64((*uint64)(&e.state), uint64(mapIsDeleted|mapIsRetired))
}
func (e *embeddedEntry[K, V]) Setup() { e.reverse, e.conflict = e.KeyHash() }

func (e *embeddedEntry[K, V]) copyFrom(src *embeddedEntry[K, V]) {
	key, value, state := src.loadTypedKeyValue()
	if stepEnabled {
		stepAt("item.copy.read", unsafe.Pointer(e.PtrListHead()), unsafe.Pointer(src.PtrListHead()))
	}
	if src.waitPayload() {
		e.initializePayload(key, value)
	}
	atomic.OrUint64((*uint64)(&e.state), uint64(state&(mapIsDummy|mapIsDeleted|mapIsPoolItem)))
	// the slot can be a published hole that a search reads
	atomic.StoreUint64(&e.conflict, atomic.LoadUint64(&src.conflict))
	atomic.StoreUint64(&e.reverse, atomic.LoadUint64(&src.reverse))
}

// Next returns the next external Entry, skipping internal embedded slots.
func (e *Entry[K, V]) Next() *Entry[K, V] {
	for next := e.embeddedEntry.Next(); next != nil; next = next.Next() {
		if entry := next.viewEntry(); entry != nil {
			return entry
		}
	}
	return nil
}

// Prev returns the previous external Entry, skipping internal embedded slots.
func (e *Entry[K, V]) Prev() *Entry[K, V] {
	for prev := e.embeddedEntry.Prev(); prev != nil; prev = prev.Prev() {
		if entry := prev.viewEntry(); entry != nil {
			return entry
		}
	}
	return nil
}
