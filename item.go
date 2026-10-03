package skiplistmap

import (
	"github.com/kazu/elist_head"
	"sync"
	"unsafe"
)

type HMapEntry[K Key[K], V any] = *Entry[K, V]
type MapItem[K Key[K], V any] = *Entry[K, V]
type entryHMap[K Key[K], V any] = embeddedEntry[K, V]
type copyEntry[K Key[K], V any] = Entry[K, V]
type SampleItem[K Key[K], V any] = Entry[K, V]

// NewEntryMap creates a caller-owned entry for immutable replacement entries.
func NewEntryMap[K Key[K], V any](key K, value V) *Entry[K, V] {
	return NewEntry(key, value)
}
func emptyEntryHMap[K Key[K], V any]() *embeddedEntry[K, V] { return nil }
func EmptyEntryHMap[K Key[K], V any]() *Entry[K, V]         { return nil }
func emptyBucket[K Key[K], V any]() *bucket[K, V]           { return nil }

func entryHMapFromPlistHead[K Key[K], V any](head unsafe.Pointer) *embeddedEntry[K, V] {
	return entryHMapFromListHead[K, V]((*elist_head.ListHead)(head))
}
func entryHMapFromListHead[K Key[K], V any](head *elist_head.ListHead) *embeddedEntry[K, V] {
	if head == nil || mapheadFromLListHead(head).IsDummy() {
		return nil
	}
	return mapheadFromLListHead(head).recoverEntry[K, V]()
}
func entryHMapOffset[K Key[K], V any]() uintptr {
	var e Entry[K, V]
	return e.Offset()
}

type CondOfFinder[K Key[K], V any] func(ehead *Entry[K, V]) bool

func CondOfFind[K Key[K], V any](reverse uint64, l sync.Locker) CondOfFinder[K, V] {

	return func(ehead *Entry[K, V]) bool {

		if EnableStats {
			l.Lock()
			DebugStats[CntSearchEntry]++
			l.Unlock()
		}
		return reverse <= ehead.reverse
	}

}

type entryBuffer[K Key[K], V any] struct {
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
