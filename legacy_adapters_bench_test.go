package skiplistmap_test

import (
	"github.com/cornelk/hashmap"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/lrita/cmap"
)

// Value adapters preserve the master method bodies and value receivers.
// Reference adapters differ only in receiver type.
type legacyHashMap struct {
	m *hashmap.HashMap
}

func (m legacyHashMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Get(k)
	v = inf.(*list_head.ListHead)
	return v, ok
}

func (m legacyHashMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Set(k, v)
	return true
}

type legacyCMap struct {
	m cmap.Cmap
}

func (m legacyCMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Load(k)
	v, ok = inf.(*list_head.ListHead)
	return v, ok
}

func (m legacyCMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Store(k, v)
	return true
}

type referenceHashMap struct {
	m *hashmap.HashMap
}

func (m *referenceHashMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Get(k)
	v = inf.(*list_head.ListHead)
	return v, ok
}

func (m *referenceHashMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Set(k, v)
	return true
}

type referenceCMap struct {
	m cmap.Cmap
}

func (m *referenceCMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Load(k)
	v, ok = inf.(*list_head.ListHead)
	return v, ok
}

func (m *referenceCMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Store(k, v)
	return true
}
