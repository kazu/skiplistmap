package skiplistmap_test

import (
	"fmt"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// Legacy benchmark copied from master. Keep its worker assignment, key mask,
// case list and unchecked operations separate from Benchmark_MapPerOperation.
// Only module paths, generic API arguments and private names are adapted.
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
		{"sync.Map                     ", 100, 100000, 0, 0x000, 0, legacySyncMap{}, true},

		{"skiplistmap4    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		{"skiplistmap4    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 100, 100000, 0, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},

		// use
		//{"hashmap.HashMap              ", 100, 100000, 0, 0x000, 0, hashMap{m: &hashmap.HashMap{}}},
		//{"cmap.Cmap              	   ", 100, 100000, 0, 0x000, 0, cMap{}},

		// {"skiplistmap                  ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},
		// {"skiplistmap3                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},
		//{"RMap                         ", 100, 100000, 0, 0x000, 0, newWRMap()},

		// {"skiplistmap                  ", 100, 100000, 0, 0x008, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},

		// {"WithLock                     ", 100, 100000, 10, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 10, 0x000, 0, legacySyncMap{}},
		// {"skiplistmap nestsearch       ", 100, 100000, 10, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},
		// {"skiplistmap                  ", 100, 100000, 10, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())},

		// use
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, true},
		{"sync.Map        ", 100, 100000, 50, 0x000, 0, legacySyncMap{}, true},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), true},
		{"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), true},
		{"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), true},
		{"mapWithMutex    ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}, false},
		{"sync.Map        ", 100, 100000, 50, 0x000, 0, legacySyncMap{}, false},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		{"skiplistmap5    ", 100, 100000, 50, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true))), false},
		// use
		// {"skiplistmap4    ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		// {"skiplistmap4    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]()), false},
		// {"hashmap.HashMap              ", 100, 100000, 50, 0x000, 0, hashMap{m: &hashmap.HashMap{}}},
		// {"cmap.Cmap              	   ", 100, 100000, 50, 0x000, 0, cMap{}},

		//{"RMap                         ", 100, 100000, 50, 0x000, 0, newWRMap()},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](bm.buckets)(whmap.base)
				skiplistmap.BucketMode[skiplistmap.StringKey, any](bm.mode)(whmap.base)
			}
			runBnech(b, &bm)
		})
	}

}
