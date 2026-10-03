package skiplistmap_test

import (
	"fmt"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cornelk/hashmap"
)

// Legacy benchmark copied from master. Keep its worker assignment, key mask,
// case list and unchecked operations separate from Benchmark_MapPerOperation.
// Module paths and private names are adapted; skiplistmap values use *ListHead
// as requested. Worker assignment, key selection and operation loops are unchanged.
type legacyMapTestParam struct {
	name       string
	concurrent int
	cnt        int
	percent    int
	buckets    int
	mode       skiplistmap.SearchMode
	mapInf     list_head.MapGetSet
	isUpdate   bool
}

func (p *legacyMapTestParam) String() string {
	return fmt.Sprintf("%s w/%3d u/%v bucket=%3d", p.name, p.percent, p.isUpdate, p.buckets)
}

type legacyBenchParam func(*legacyMapTestParam) legacyBenchParam

func (p *legacyMapTestParam) Option(opts ...legacyBenchParam) (prevs []legacyBenchParam) {

	for _, opt := range opts {
		prevs = append(prevs, opt(p))
	}
	return
}

func runBnech(b *testing.B, param *legacyMapTestParam, opts ...legacyBenchParam) {

	m := param.mapInf
	concurretRoutine := param.concurrent
	_ = concurretRoutine
	operationCnt := param.cnt
	pctWrites := uint64(param.percent)
	isUpdate := param.isUpdate

	b.ReportAllocs()
	size := operationCnt
	mask := size - 1
	rc := uint64(0)

	for j := 0; j < operationCnt; j++ {
		m.Set(fmt.Sprintf("%d", j), &list_head.ListHead{})
	}
	if rmap, ok := m.(*list_head.RMap); ok {
		_ = rmap
		//rmap.ValidateDirty()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := rand.Int() & mask
		mc := atomic.AddUint64(&rc, 1)

		if pctWrites*mc/100 != pctWrites*(mc-1)/100 {
			for pb.Next() {
				if isUpdate {
					m.Set(fmt.Sprintf("%d", index&mask), &list_head.ListHead{})
				} else {
					m.Set(fmt.Sprintf("xx%dxx", index&mask), &list_head.ListHead{})
				}
				index = index + 1
			}
		} else {
			for pb.Next() {
				m.Get(fmt.Sprintf("%d", index&mask))
				// if !ok {
				// 	_, ok = m.Get(fmt.Sprintf("%d", index&mask))
				// 	fmt.Printf("fail")
				// }
				index = index + 1
			}
		}
	})

}

type legacySyncMap struct {
	m sync.Map
}

func (m legacySyncMap) Get(k string) (v *list_head.ListHead, ok bool) {

	ov, ok := m.m.Load(k)
	v, ok = ov.(*list_head.ListHead)
	return
}

func (m legacySyncMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Store(k, v)
	return true

}

func Benchmark_Map(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []legacyMapTestParam{
		// use
		{"mapWithMutex                 ", 100, 100000, 0, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map.value                     ", 100, 100000, 0, 0x000, 0, legacySyncMap{}, true},
		{"sync.Map.reference                     ", 100, 100000, 0, 0x000, 0, &syncMap{}, true},

		{"skiplistmap4    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, false},
		{"skiplistmap4    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, false},
		{"skiplistmap5    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},
		{"skiplistmap5    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},
		{"skiplistmap5    ", 100, 100000, 0, 0x040, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},
		{"skiplistmap5    ", 100, 100000, 0, 0x080, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},

		// use
		{"hashmap.HashMap              ", 100, 100000, 0, 0x000, 0, legacyHashMap{m: &hashmap.HashMap{}}, true},
		{"cmap.value              	   ", 100, 100000, 0, 0x000, 0, legacyCMap{}, true},
		{"cmap.reference              	   ", 100, 100000, 0, 0x000, 0, &referenceCMap{}, true},

		// {"skiplistmap                  ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},
		//{"RMap                         ", 100, 100000, 0, 0x000, 0, newWRMap()},

		// {"skiplistmap                  ", 100, 100000, 0, 0x008, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},

		// {"WithLock                     ", 100, 100000, 10, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 10, 0x000, 0, legacySyncMap{}},
		// {"skiplistmap nestsearch       ", 100, 100000, 10, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},
		// {"skiplistmap                  ", 100, 100000, 10, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]())},

		// use
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map.value        ", 100, 100000, 50, 0x000, 0, legacySyncMap{}, true},
		{"sync.Map.reference        ", 100, 100000, 50, 0x000, 0, &syncMap{}, true},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, true},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, true},
		{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, true},
		{"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, true},
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, false},
		{"sync.Map.value        ", 100, 100000, 50, 0x000, 0, legacySyncMap{}, false},
		{"sync.Map.reference        ", 100, 100000, 50, 0x000, 0, &syncMap{}, false},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](true))}, false},
		// use
		// {"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, false},
		// {"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead]()}, false},
		{"hashmap.HashMap              ", 100, 100000, 50, 0x000, 0, legacyHashMap{m: &hashmap.HashMap{}}, true},
		{"cmap.value              	   ", 100, 100000, 50, 0x000, 0, legacyCMap{}, true},
		{"cmap.reference              	   ", 100, 100000, 50, 0x000, 0, &referenceCMap{}, true},

		//{"RMap                         ", 100, 100000, 50, 0x000, 0, newWRMap()},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*legacyTypedMap); ok {
				skiplistmap.MaxPefBucket[skiplistmap.StringKey, *list_head.ListHead](bm.buckets)(whmap.base)
				skiplistmap.BucketMode[skiplistmap.StringKey, *list_head.ListHead](bm.mode)(whmap.base)
			}
			runBnech(b, &bm)
		})
	}

}
