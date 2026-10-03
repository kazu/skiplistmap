package skiplistmap_test

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/cornelk/hashmap"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"github.com/kazu/skiplistmap/rmap"
	"github.com/lrita/cmap"
)

type mapTestParam struct {
	name       string
	concurrent int
	cnt        int
	percent    int
	buckets    int
	mode       skiplistmap.SearchMode
	mapInf     list_head.MapGetSet
	isUpdate   bool
}

func (p *mapTestParam) String() string {
	return fmt.Sprintf("%s w/%3d u/%v bucket=%3d workers=%d", p.name, p.percent, p.isUpdate, p.buckets, p.concurrent)
}

type WRMap struct {
	base *rmap.RMap[skiplistmap.StringKey, *list_head.ListHead]
}

func (w *WRMap) Set(k string, v *list_head.ListHead) bool {
	return w.base.Set(skiplistmap.StringKey(k), v)

}

func (w *WRMap) Get(k string) (v *list_head.ListHead, ok bool) {
	return w.base.Get(skiplistmap.StringKey(k))
}

func newWRMap() *WRMap {
	return &WRMap{
		base: rmap.New[skiplistmap.StringKey, *list_head.ListHead](),
	}
}

type WrapHMap struct {
	base *skiplistmap.Map[skiplistmap.StringKey, any]
}

func (w *WrapHMap) Set(k string, v *list_head.ListHead) bool {

	//return w.base.StoreItem(&skiplistmap.SampleItem{K: k, V: v})
	return w.base.Set(skiplistmap.StringKey(k), v)
}

func (w *WrapHMap) Delete(k string) bool {

	return w.base.Purge(skiplistmap.StringKey(k))
}

func (w *WrapHMap) Get(k string) (v *list_head.ListHead, ok bool) {
	result, ok := w.base.GetByHash(skiplistmap.MemHashString(k), xxhash.Sum64String(k))
	if !ok {
		return nil, ok
	}
	v = result.(*list_head.ListHead)
	return
}

func newWrapHMap(hmap *skiplistmap.Map[skiplistmap.StringKey, any]) *WrapHMap {

	return &WrapHMap{base: hmap}
}

type BenchParam func(*mapTestParam) BenchParam

func (p *mapTestParam) Option(opts ...BenchParam) (prevs []BenchParam) {

	for _, opt := range opts {
		prevs = append(prevs, opt(p))
	}
	return
}

func runBenchPerOperation(b *testing.B, param *mapTestParam, opts ...BenchParam) {
	m := param.mapInf
	size := param.cnt
	var workers, failedWrites atomic.Uint64

	for j := 0; j < size; j++ {
		if !m.Set(fmt.Sprintf("%d", j), &list_head.ListHead{}) {
			b.Fatal("initial Set failed")
		}
	}
	parallelism := (param.concurrent + runtime.GOMAXPROCS(0) - 1) / runtime.GOMAXPROCS(0)
	b.SetParallelism(parallelism)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		worker := int(workers.Add(1) - 1)
		index := worker % size
		operation := worker % 100
		for pb.Next() {
			if operation < param.percent {
				key := fmt.Sprintf("%d", index)
				if !param.isUpdate {
					key = fmt.Sprintf("xx%dxx", index)
				}
				if !m.Set(key, &list_head.ListHead{}) {
					failedWrites.Add(1)
				}
			} else {
				if _, ok := m.Get(fmt.Sprintf("%d", index)); !ok {
					b.Error("Get of a populated key failed")
					return
				}
			}
			index = (index + 1) % size
			operation = (operation + 1) % 100
		}
	})
	b.ReportMetric(float64(failedWrites.Load())/float64(b.N), "failed-writes/op")
}

type syncMap struct {
	m sync.Map
}

func (m *syncMap) Get(k string) (v *list_head.ListHead, ok bool) {

	ov, ok := m.m.Load(k)
	v, ok = ov.(*list_head.ListHead)
	return
}

func (m *syncMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Store(k, v)
	return true
}

type hashMap struct {
	m *hashmap.HashMap
}

func (m hashMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Get(k)
	v = inf.(*list_head.ListHead)
	return v, ok
}

func (m hashMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Set(k, v)
	return true
}

type cMap struct {
	m cmap.Cmap
}

func (m *cMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Load(k)
	v, ok = inf.(*list_head.ListHead)
	return v, ok
}

func (m *cMap) Set(k string, v *list_head.ListHead) (ok bool) {

	m.m.Store(k, v)
	return true
}

func Benchmark_HMap_forProfile(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []mapTestParam{
		//{"skiplistmap4    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		//{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		//{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), true},
		//{"skiplistmap5    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap5    ", 64, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		//{"skiplistmap5    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), true},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](bm.buckets)(whmap.base)
				skiplistmap.BucketMode[skiplistmap.StringKey, any](bm.mode)(whmap.base)
			}
			runBenchPerOperation(b, &bm)
		})
	}
}
func Benchmark_MapPerOperation(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []mapTestParam{
		// use
		{"mapWithMutex                 ", 64, 100000, 0, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map.value                     ", 64, 100000, 0, 0x000, 0, legacySyncMap{}, true},
		{"sync.Map.reference                     ", 64, 100000, 0, 0x000, 0, &syncMap{}, true},

		{"skiplistmap4    ", 64, 100000, 0, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		{"skiplistmap4    ", 64, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		{"skiplistmap5    ", 64, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 64, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 64, 100000, 0, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 64, 100000, 0, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},

		// use
		{"cmap.value", 64, 100000, 0, 0x000, 0, legacyCMap{}, true},
		{"cmap.reference", 64, 100000, 0, 0x000, 0, &referenceCMap{}, true},
		{"hashmap.value", 64, 100000, 0, 0x000, 0, legacyHashMap{m: &hashmap.HashMap{}}, true},
		{"hashmap.reference", 64, 100000, 0, 0x000, 0, &referenceHashMap{m: &hashmap.HashMap{}}, true},

		// {"skiplistmap                  ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		{"RMap                         ", 64, 100000, 0, 0x000, 0, newWRMap(), false},

		// {"skiplistmap                  ", 100, 100000, 0, 0x008, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		// {"WithLock                     ", 100, 100000, 10, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 10, 0x000, 0, &syncMap{}},
		// {"skiplistmap nestsearch       ", 100, 100000, 10, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap                  ", 100, 100000, 10, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		// use
		{"mapWithMutex    ", 64, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map.value        ", 64, 100000, 50, 0x000, 0, legacySyncMap{}, true},
		{"sync.Map.reference        ", 64, 100000, 50, 0x000, 0, &syncMap{}, true},
		{"skiplistmap5    ", 64, 100000, 50, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap5    ", 64, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap5    ", 64, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap5    ", 64, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap4    ", 64, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), true},
		{"skiplistmap4    ", 64, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), true},
		{"mapWithMutex    ", 64, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, false},
		{"sync.Map.value        ", 64, 100000, 50, 0x000, 0, legacySyncMap{}, false},
		{"sync.Map.reference        ", 64, 100000, 50, 0x000, 0, &syncMap{}, false},
		{"skiplistmap5    ", 64, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 64, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		// use
		// {"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		// {"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		{"cmap.value", 64, 100000, 50, 0x000, 0, legacyCMap{}, true},
		{"cmap.reference", 64, 100000, 50, 0x000, 0, &referenceCMap{}, true},
		{"hashmap.value", 64, 100000, 50, 0x000, 0, legacyHashMap{m: &hashmap.HashMap{}}, true},
		{"hashmap.reference", 64, 100000, 50, 0x000, 0, &referenceHashMap{m: &hashmap.HashMap{}}, true},
		{"cmap.value", 64, 100000, 50, 0x000, 0, legacyCMap{}, false},
		{"cmap.reference", 64, 100000, 50, 0x000, 0, &referenceCMap{}, false},
		{"hashmap.value", 64, 100000, 50, 0x000, 0, legacyHashMap{m: &hashmap.HashMap{}}, false},
		{"hashmap.reference", 64, 100000, 50, 0x000, 0, &referenceHashMap{m: &hashmap.HashMap{}}, false},

		{"RMap                         ", 64, 100000, 50, 0x000, 0, newWRMap(), false},
		{"RMap                         ", 64, 100000, 50, 0x000, 0, newWRMap(), true},
	}

	for _, bm := range benchmarks {
		if _, ok := bm.mapInf.(*WrapHMap); ok {
			bm.name = strings.TrimSpace(bm.name) + "_typed"
			bm.mapInf = newTypedBenchmarkMap(bm.mode, bm.buckets)
			benchmarks = append(benchmarks, bm)
		}
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](bm.buckets)(whmap.base)
				skiplistmap.BucketMode[skiplistmap.StringKey, any](bm.mode)(whmap.base)
			}
			runBenchPerOperation(b, &bm)
		})
	}

}

func Benchmark_HMap(b *testing.B) {

	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []mapTestParam{
		// {"HMap               ", 100, 100000, 0, 0x020, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x040, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x080, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x100, list_head.NewHMap()},

		{"HMap               ", 64, 100000, 0, 0x200, skiplistmap.LenearSearchForBucket, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},

		// // {"HMap               ", 100, 100000, 0, 0x258, list_head.LenearSearchForBucket, list_head.NewHMap()},
		// // {"HMap               ", 100, 100000, 0, 0x400, list_head.LenearSearchForBucket, list_head.NewHMap()},

		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x040, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x080, list_head.NestedSearchForBucket, list_head.NewHMap()},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x010, list_head.NestedSearchForBucket, list_head.NewHMap()},

		{"HMap_nestsearch    ", 64, 100000, 0, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		{"HMap_combine       ", 64, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		//{"HMap_combine       ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		{"HMap_combine2      ", 64, 100000, 0, 0x010, skiplistmap.CombineSearch2, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x400, list_head.NestedSearchForBucket, list_head.NewHMap()},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.NoItemSearchForBucket, list_head.NewHMap()},
		// {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.FalsesSearchForBucket, list_head.NewHMap()},

		// {"HMap               ", 100, 200000, 0, 0x200, list_head.NewHMap()},
		// {"HMap               ", 100, 200000, 0, 0x300, list_head.NewHMap()},

		//		{"HMap               ", 100, 100000, 50, list_head.NewHMap()},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](bm.buckets)(whmap.base)
				skiplistmap.BucketMode[skiplistmap.StringKey, any](bm.mode)(whmap.base)
			}
			runBenchPerOperation(b, &bm)
		})
	}

}

type TestElm struct {
	I int
	J int
}

func Benchmark_slice_vs_unsafe(b *testing.B) {

	makeSlice := func(cnt int) []TestElm {

		slice := make([]TestElm, cnt)
		for i := range slice {
			slice[i].J = i
		}
		return slice
	}
	const size int = int(unsafe.Sizeof(TestElm{}))

	benchmarks := []struct {
		name   string
		cnt    int
		travFn func([]TestElm, unsafe.Pointer, int) int
	}{
		{
			"slice traverse",
			100000,
			func(slice []TestElm, f unsafe.Pointer, i int) int {

				return slice[i].J

			},
		}, {
			"unsafe traverse",
			100000,
			func(slice []TestElm, f unsafe.Pointer, i int) int {
				pj := (*int)(unsafe.Add(f, i*size))
				return *pj
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {

			slice := makeSlice(bm.cnt)
			first := unsafe.Pointer(&slice[0].J)
			b.ResetTimer()

			for jj := 0; jj < b.N; jj++ {
				b.StartTimer()
				for i := range slice {
					bm.travFn(slice, first, i)
				}
				b.StopTimer()
			}

		})
	}

}
