package typedapi

import (
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

type Entry[K Key[K], V any] struct {
	key   K
	value V
	skiplistmap.MapHead
}

func NewEntry[K Key[K], V any](key K, value V) *Entry[K, V] {
	e := &Entry[K, V]{key: key, value: value}
	e.ListHead.Init()
	return e
}

func (e *Entry[K, V]) Key() K   { return e.key }
func (e *Entry[K, V]) Value() V { return e.value }

// EntryView reuses elist_head's typed link conversion.
type EntryView[K Key[K], V any] struct {
	elist_head.List[Entry[K, V]]
}

func NewEntryView[K Key[K], V any]() EntryView[K, V] {
	var e Entry[K, V]
	return EntryView[K, V]{elist_head.NewList[Entry[K, V]](unsafe.Offsetof(e.MapHead) + unsafe.Offsetof(e.MapHead.ListHead))}
}

// RecoverField probes a generic method for container_of within an inline V.
// field must address a field at offset in the V of a live Entry[K,V]. It must
// not address a copy of V or a separately allocated value pointed to by V.
func (v EntryView[K, V]) RecoverField[F any](field *F, offset uintptr) *Entry[K, V] {
	var e Entry[K, V]
	return (*Entry[K, V])(unsafe.Add(unsafe.Pointer(field), -int(unsafe.Offsetof(e.value)+offset)))
}
