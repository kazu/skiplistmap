package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/cornelk/hashmap"
	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"github.com/kazu/skiplistmap/rmap"
	"github.com/lrita/cmap"
	"github.com/stretchr/testify/assert"
)

func runBnech(b *testing.B, m list_head.MapGetSet, concurretRoutine, operationCnt int, pctWrites uint64) {

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

	isUpdate := true
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

func Test_HmapEntry(t *testing.T) {

	tests := []struct {
		name   string
		wanted skiplistmap.MapItem
		got    func(*elist_head.ListHead) skiplistmap.MapItem
	}{
		{
			name:   "SampleItem",
			wanted: &skiplistmap.SampleItem{K: "hoge", V: "hoge value"},
			got: func(lhead *elist_head.ListHead) skiplistmap.MapItem {
				return (skiplistmap.EmptySampleHMapEntry).HmapEntryFromListHead(lhead).(skiplistmap.MapItem)
			},
		},
		{
			name:   "entryHMap",
			wanted: skiplistmap.NewEntryMap("hogeentry", "hogevalue"),
			got: func(lhead *elist_head.ListHead) skiplistmap.MapItem {
				return (skiplistmap.EmptyEntryHMap).HmapEntryFromListHead(lhead).(skiplistmap.MapItem)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wanted.Key(), tt.got(tt.wanted.PtrListHead()).Key())

		})
	}

}

func Test_ConccurentWriteEmbeddedBucket(t *testing.T) {

	m := skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32),
		skiplistmap.UseEmbeddedPool(true),
		skiplistmap.BucketMode(skiplistmap.CombineSearch3))

	tests := []struct {
		r uint64
	}{
		{0x100007800a100000},
		{0x100007800a200000},
		{0x100007800a300000},
		{0x100007800a400000},
	}

	var wg sync.WaitGroup

	for i, t := range tests {
		wg.Add(1)
		go func(idx int, key uint64) {
			//func(idx int, key uint64) {
			for i := uint64(1); i < 10; i++ {
				bucket := m.FindBucket(key + i)
				item, pool, fn := bucket.GetItem(key + i)
				_ = fn
				if pool != nil {
					//panic("nPool is not extend")
					bucket.SetItemPool(pool)
				}

				s := item.(*skiplistmap.SampleItem)
				s.K = "???"
				s.V = nil
				m.TestSet(bits.Reverse64(key+i), i, bucket, s)
				if fn != nil {
					bucket.RunLazyUnlocker(fn)
				}
			}
			wg.Done()
		}(i, t.r)
	}
	wg.Wait()

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

func Test_HMap(t *testing.T) {

	tests := []struct {
		name string
		m    *WrapHMap
	}{
		{
			"embedded pool",
			newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UseEmbeddedPool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3))),
		},
		// {
		// 	"pool without goroutine",
		// 	newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3))),
		// },
		// {
		// 	"combine4",
		// 	newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch4))),
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.m
			m.Set("hoge", &list_head.ListHead{})
			v := &list_head.ListHead{}
			v.Init()
			m.Set("hoge1", v)
			m.Set("hoge3", v)
			m.Set("oge3", v)
			m.Set("3", v)
			DumpHmap(m.base)
			for i := 0; i < 100000; i++ {
				m.Set(fmt.Sprintf("fuge%d", i), v)
			}
			skiplistmap.ResetStats()

			_, success := m.Get("hoge1")

			assert.True(t, success)

			_, success = m.Get("1234")

			assert.False(t, success)

			for i := 0; i < 100000; i++ {
				_, ok := m.Get(fmt.Sprintf("fuge%d", i))
				assert.Truef(t, ok, "not found key=%s", fmt.Sprintf("fuge%d", i))
				if !ok {
					skiplistmap.BucketMode(skiplistmap.NestedSearchForBucket)(m.base)
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
					skiplistmap.BucketMode(skiplistmap.CombineSearch)(m.base)
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
				}
			}

			e := m.base.Last()
			_ = e
			assert.Equal(t, ^uint64(0), e.PtrMapHead().KeyInHmap())
		})

	}
}

func DumpHmap(h *skiplistmap.Map) {

	fmt.Printf("---DumpBucketPerLevel---\n")
	h.DumpBucketPerLevel(nil)
	fmt.Printf("---DumpBucket---\n")
	h.DumpBucket(nil)
	h.DumpEntry(nil)
	fmt.Printf("fail 0x%x\n", skiplistmap.Failreverse)

}

func Benchmark_HMap_forProfile(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []struct {
		name       string
		concurrent int
		cnt        int
		percent    int
		buckets    int
		mode       skiplistmap.SearchMode
		mapInf     list_head.MapGetSet
	}{
		//{"RMap            ", 100, 100000, 50, 0x000, 0, newWRMap()},
		{"skiplistmap5    ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		// use
		//{"HMap_combine    ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},

		//{"HMap_combine3    ", 100, 100000, 0x0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},

		//{"HMap_combine    ", 100, 100000, 100, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		//{"HMap_combine2    ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch2, newWrapHMap(skiplistmap.NewHMap())},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%s w/%2d bucket=%3d", bm.name, bm.percent, bm.buckets), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, bm.mapInf, bm.concurrent, bm.cnt, uint64(bm.percent))
		})
	}
}

// func Test_KeyHash(t *testing.T) {

// 	for i := 1; i < 10000; i++ {
// 		k, _ := skiplistmap.KeyToHash(fmt.Sprintf("hoge%d", i))
// 		pk, _ := skiplistmap.KeyToHash(fmt.Sprintf("hoge%d", i-1))

// 		assert.True(t, k < pk, bits.Reverse64(k) > bits.Reverse64(pk))

// 	}

// }

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

func Benchmark_Map(b *testing.B) {
	newShard := func(fn func(int) list_head.MapGetSet) list_head.MapGetSet {
		s := &list_head.ShardMap{}
		s.InitByFn(fn)
		return s
	}
	_ = newShard

	benchmarks := []struct {
		name       string
		concurrent int
		cnt        int
		percent    int
		buckets    int
		mode       skiplistmap.SearchMode
		mapInf     list_head.MapGetSet
	}{
		// use
		// {"mapWithMutex                 ", 100, 100000, 0, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 0, 0x000, 0, syncMap{}},

		// {"skiplistmap4                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap4                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap5                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		// {"skiplistmap5                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		// {"skiplistmap5                 ", 100, 100000, 0, 0x040, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		// {"skiplistmap5                 ", 100, 100000, 0, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},

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
		// {"mapWithMutex                 ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 50, 0x000, 0, syncMap{}},
		{"skiplistmap5                 ", 100, 100000, 50, 0x080, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		{"skiplistmap5                 ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		{"skiplistmap5                 ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true)))},
		{"skiplistmap4                 ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap4                 ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},

		// use
		// {"hashmap.HashMap              ", 100, 100000, 50, 0x000, 0, hashMap{m: &hashmap.HashMap{}}},
		// {"cmap.Cmap              	   ", 100, 100000, 50, 0x000, 0, cMap{}},

		//{"RMap                         ", 100, 100000, 50, 0x000, 0, newWRMap()},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%s w/%2d bucket=%3d", bm.name, bm.percent, bm.buckets), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, bm.mapInf, bm.concurrent, bm.cnt, uint64(bm.percent))
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

	benchmarks := []struct {
		name       string
		concurrent int
		cnt        int
		percent    int
		buckets    int
		mode       skiplistmap.SearchMode
		mapInf     list_head.MapGetSet
	}{
		// {"HMap               ", 100, 100000, 0, 0x020, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x040, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x080, list_head.NewHMap()},
		// {"HMap               ", 100, 100000, 0, 0x100, list_head.NewHMap()},

		{"HMap               ", 100, 100000, 0, 0x200, skiplistmap.LenearSearchForBucket, newWrapHMap(skiplistmap.NewHMap())},

		// // {"HMap               ", 100, 100000, 0, 0x258, list_head.LenearSearchForBucket, list_head.NewHMap()},
		// // {"HMap               ", 100, 100000, 0, 0x400, list_head.LenearSearchForBucket, list_head.NewHMap()},

		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x040, list_head.NestedSearchForBucket, list_head.NewHMap()},
		// // {"HMap_nestsearch    ", 100, 100000, 0, 0x080, list_head.NestedSearchForBucket, list_head.NewHMap()},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x010, list_head.NestedSearchForBucket, list_head.NewHMap()},

		{"HMap_nestsearch    ", 100, 100000, 0, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap())},
		{"HMap_combine       ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		//{"HMap_combine       ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		{"HMap_combine2      ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch2, newWrapHMap(skiplistmap.NewHMap())},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x400, list_head.NestedSearchForBucket, list_head.NewHMap()},

		// {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.NoItemSearchForBucket, list_head.NewHMap()},
		// {"HMap_nestsearch    ", 100, 100000, 0, 0x020, list_head.FalsesSearchForBucket, list_head.NewHMap()},

		// {"HMap               ", 100, 200000, 0, 0x200, list_head.NewHMap()},
		// {"HMap               ", 100, 200000, 0, 0x300, list_head.NewHMap()},

		//		{"HMap               ", 100, 100000, 50, list_head.NewHMap()},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%s w/%2d bucket=%3d", bm.name, bm.percent, bm.buckets), func(b *testing.B) {
			if whmap, ok := bm.mapInf.(*WrapHMap); ok {
				skiplistmap.MaxPefBucket(bm.buckets)(whmap.base)
				skiplistmap.BucketMode(bm.mode)(whmap.base)
			}
			runBnech(b, bm.mapInf, bm.concurrent, bm.cnt, uint64(bm.percent))
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
