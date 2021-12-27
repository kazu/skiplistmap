package skiplistmap_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/cornelk/hashmap"
	list_head "github.com/kazu/loncha/lista_encabezado"
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
	return fmt.Sprintf("%s w/%3d u/%v bucket=%3d", p.name, p.percent, p.isUpdate, p.buckets)
}

type WRMap struct {
	base *rmap.RMap
}

func (w *WRMap) Set(k string, v *list_head.ListHead) bool {
	return w.base.Set(k, v)

}

func (w *WRMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := w.base.Get(k)
	return inf.(*list_head.ListHead), ok
}

func newWRMap() *WRMap {
	return &WRMap{
		base: rmap.New(),
	}
}

type WrapHMap struct {
	base *skiplistmap.Map
}

func (w *WrapHMap) Set(k string, v *list_head.ListHead) bool {

	//return w.base.StoreItem(&skiplistmap.SampleItem{K: k, V: v})
	return w.base.Set(k, v)
}

func (w *WrapHMap) Delete(k string) bool {

	return w.base.Delete(k)
}

func (w *WrapHMap) Get(k string) (v *list_head.ListHead, ok bool) {
	result, ok := w.base.LoadItemByHash(skiplistmap.MemHashString(k), xxhash.Sum64String(k))
	if !ok || result == nil {
		return nil, ok
	}
	v = result.Value().(*list_head.ListHead)
	return
}

func newWrapHMap(hmap *skiplistmap.Map) *WrapHMap {
	skiplistmap.ItemFn(func() skiplistmap.MapItem {
		return skiplistmap.EmptySampleHMapEntry
	})(hmap)

	return &WrapHMap{base: hmap}
}

type BenchParam func(*mapTestParam) BenchParam

func (p *mapTestParam) Option(opts ...BenchParam) (prevs []BenchParam) {

	for _, opt := range opts {
		prevs = append(prevs, opt(p))
	}
	return
}

func runBnech(b *testing.B, param *mapTestParam, opts ...BenchParam) {

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

type syncMap struct {
	m sync.Map
}

func (m syncMap) Get(k string) (v *list_head.ListHead, ok bool) {

	ov, ok := m.m.Load(k)
	v, ok = ov.(*list_head.ListHead)
	return
}

func (m syncMap) Set(k string, v *list_head.ListHead) (ok bool) {

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

func (m cMap) Get(k string) (v *list_head.ListHead, ok bool) {
	inf, ok := m.m.Load(k)
	v, ok = inf.(*list_head.ListHead)
	return v, ok
}

func (m cMap) Set(k string, v *list_head.ListHead) (ok bool) {

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
		{"skiplistmap5    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		//{"skiplistmap5    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), true},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, &bm)
		})
	}
}
func Benchmark_Map(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []mapTestParam{
		// use
		{"mapWithMutex                 ", 100, 100000, 0, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map                     ", 100, 100000, 0, 0x000, 0, syncMap{}, true},

		{"skiplistmap4    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		{"skiplistmap4    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},

		// use
		//{"hashmap.HashMap              ", 100, 100000, 0, 0x000, 0, hashMap{m: &hashmap.HashMap{}}},
		//{"cmap.Cmap              	   ", 100, 100000, 0, 0x000, 0, cMap{}},

		// {"skiplistmap                  ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		//{"RMap                         ", 100, 100000, 0, 0x000, 0, newWRMap()},

		// {"skiplistmap                  ", 100, 100000, 0, 0x008, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		// {"WithLock                     ", 100, 100000, 10, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 10, 0x000, 0, syncMap{}},
		// {"skiplistmap nestsearch       ", 100, 100000, 10, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap                  ", 100, 100000, 10, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		// use
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map        ", 100, 100000, 50, 0x000, 0, syncMap{}, true},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), true},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), true},
		{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), true},
		{"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), true},
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, false},
		{"sync.Map        ", 100, 100000, 50, 0x000, 0, syncMap{}, false},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true))), false},
		{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		{"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap()), false},
		// use
		// {"hashmap.HashMap              ", 100, 100000, 50, 0x000, 0, hashMap{m: &hashmap.HashMap{}}},
		// {"cmap.Cmap              	   ", 100, 100000, 50, 0x000, 0, cMap{}},

		//{"RMap                         ", 100, 100000, 50, 0x000, 0, newWRMap()},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, &bm)
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

		{"HMap               ", 100, 100000, 0, 0x200, skiplistmap.LenearSearchForBucket, newWrapHMap(skiplistmap.NewHMap()), false},

		// // {"HMap               ", 100, 100000, 0, 0x258, list_head.LenearSearchForBucket, list_head.NewHMap()},
		// // {"HMap               ", 100, 100000, 0, 0x400, list_head.LenearSearchForBucket, list_head.NewHMap()},

		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x040, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x080, list_head.NestedSearchForBucket, list_head.NewHMap()},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x010, list_head.NestedSearchForBucket, list_head.NewHMap()},

		{"HMap_nestsearch    ", 100, 100000, 0, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap()), false},
		{"HMap_combine       ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap()), false},
		//{"HMap_combine       ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		{"HMap_combine2      ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch2, newWrapHMap(skiplistmap.NewHMap()), false},

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
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, &bm)
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
