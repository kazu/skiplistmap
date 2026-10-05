package skiplistmap

import (
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
)

// listaList is the typed view of a list whose nodes embed a
// list_head.ListHead at offset in T, like elist_head.List for elist_head: it
// stores the offset only. The links of lista_encabezado are pointers, so the
// list keeps its elements alive by itself.
type listaList[T any] struct {
	offset uintptr
}

func newListaList[T any](offset uintptr) listaList[T] {
	return listaList[T]{offset: offset}
}

// Link returns the embedded link of v.
func (l listaList[T]) Link(v *T) *list_head.ListHead {
	return (*list_head.ListHead)(unsafe.Add(unsafe.Pointer(v), l.offset))
}

// Element returns the element that h is embedded in; h must be the link of
// a T, not a head or tail of the list.
func (l listaList[T]) Element(h *list_head.ListHead) *T {
	return (*T)(unsafe.Add(unsafe.Pointer(h), -int(l.offset)))
}

func (l listaList[T]) Next(v *T) *T {
	return l.Element(l.Link(v).Next())
}

func (l listaList[T]) Prev(v *T) *T {
	return l.Element(l.Link(v).Prev())
}

// InsertBefore links v just before at, a link of the list or its tail.
func (l listaList[T]) InsertBefore(at *list_head.ListHead, v *T) error {
	_, err := at.InsertBefore(l.Link(v))
	return err
}

func (l listaList[T]) MarkForDelete(v *T) error {
	return l.Link(v).MarkForDelete()
}
