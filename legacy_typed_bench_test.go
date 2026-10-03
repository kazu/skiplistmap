package skiplistmap_test

import (
	"github.com/cespare/xxhash"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// legacyTypedMap preserves the old hash-based calling path with typed values.
// The removed ItemFn factory is supplied by the typed Map implementation.
// LoadItemByHash is forbidden for embedded pools, so GetByHash is used.
type legacyTypedMap struct {
	base *skiplistmap.Map[skiplistmap.StringKey, *list_head.ListHead]
}

func (w *legacyTypedMap) Set(k string, v *list_head.ListHead) bool {
	return w.base.Set(skiplistmap.StringKey(k), v)
}
func (w *legacyTypedMap) Delete(k string) bool {
	return w.base.Purge(skiplistmap.StringKey(k))
}
func (w *legacyTypedMap) Get(k string) (v *list_head.ListHead, ok bool) {
	return w.base.GetByHash(skiplistmap.MemHashString(k), xxhash.Sum64String(k))
}
