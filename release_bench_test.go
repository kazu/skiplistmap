package skiplistmap_test

import (
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

type typedBenchmarkMap struct {
	base *skiplistmap.Map[skiplistmap.StringKey, *list_head.ListHead]
}

func newTypedBenchmarkMap(mode skiplistmap.SearchMode, buckets int) *typedBenchmarkMap {
	return &typedBenchmarkMap{base: skiplistmap.New[skiplistmap.StringKey, *list_head.ListHead](
		skiplistmap.BucketMode[skiplistmap.StringKey, *list_head.ListHead](mode),
		skiplistmap.MaxPefBucket[skiplistmap.StringKey, *list_head.ListHead](buckets),
		skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](mode == skiplistmap.CombineSearch3),
	)}
}

func (m *typedBenchmarkMap) Get(k string) (*list_head.ListHead, bool) {
	return m.base.Get(skiplistmap.StringKey(k))
}

func (m *typedBenchmarkMap) Set(k string, v *list_head.ListHead) bool {
	return m.base.Set(skiplistmap.StringKey(k), v)
}
