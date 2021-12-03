package skiplistmap_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"github.com/kazu/skiplistmap/rmap"
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

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3)))

	//m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.BucketMode(skiplistmap.CombineSearch3)))
	//m := list_head.NewHMap(list_head.MaxPefBucket(32), list_head.BucketMode(list_head.NestedSearchForBucket))
	//m := list_head.NewHMap(list_head.MaxPefBucket(32), list_head.BucketMode(list_head.LenearSearchForBucket))
	skiplistmap.EnableStats = true
	//levels := m.base.ActiveLevels()
	//assert.Equal(t, 0, len(levels))
	a := skiplistmap.MapHead{}
	_ = a
	m.Set("hoge", &list_head.ListHead{})

	v := &list_head.ListHead{}
	v.Init()
	m.Set("hoge1", v)
	m.Set("hoge3", v)
	m.Set("oge3", v)
	m.Set("3", v)
	//m.ValidateDirty()

	// levels = m.base.ActiveLevels()
	// assert.Equal(t, 2, len(levels))

	for i := 0; i < 10000; i++ {
		m.Set(fmt.Sprintf("fuge%d", i), v)
	}
	skiplistmap.ResetStats()

	_, success := m.Get("hoge1")

	assert.True(t, success)
	stat := skiplistmap.DebugStats
	//DumpHmap(m.base)

	_, success = m.Get("1234")

	assert.False(t, success)
	stat = skiplistmap.DebugStats
	_ = stat
	conf := list_head.DefaultModeTraverse
	_ = conf

	for i := 0; i < 10000; i++ {
		_, ok := m.Get(fmt.Sprintf("fuge%d", i))
		assert.Truef(t, ok, "not found key=%s", fmt.Sprintf("fuge%d", i))
		if !ok {
			skiplistmap.BucketMode(skiplistmap.NestedSearchForBucket)(m.base)
			_, ok = m.Get(fmt.Sprintf("fuge%d", i))
			skiplistmap.BucketMode(skiplistmap.CombineSearch)(m.base)
			_, ok = m.Get(fmt.Sprintf("fuge%d", i))
		}
	}
	conf = list_head.DefaultModeTraverse
	_ = conf
	DumpHmap(m.base)

	var b strings.Builder
	m.base.DumpEntry(&b)
	//assert.Equal(t, 0, len(b.String()))
	assert.NotEqual(t, 0, len(b.String()))

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

		{"HMap_combine    ", 100, 100000, 0x0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
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
		{"mapWithMutex                 ", 100, 100000, 0, 0x000, 0, &list_head.MapWithLock{}},
		{"sync.Map                     ", 100, 100000, 0, 0x000, 0, syncMap{}},
		{"skiplistmap                  ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap3                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap3                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap4                 ", 100, 100000, 0, 0x010, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap4                 ", 100, 100000, 0, 0x020, skiplistmap.CombineSearch4, newWrapHMap(skiplistmap.NewHMap())},
		{"RMap                         ", 100, 100000, 0, 0x000, 0, newWRMap()},

		// {"skiplistmap                  ", 100, 100000, 0, 0x008, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		// {"WithLock                     ", 100, 100000, 10, 0x000, 0, &list_head.MapWithLock{}},
		// {"sync.Map                     ", 100, 100000, 10, 0x000, 0, syncMap{}},
		// {"skiplistmap nestsearch       ", 100, 100000, 10, 0x020, skiplistmap.NestedSearchForBucket, newWrapHMap(skiplistmap.NewHMap())},
		// {"skiplistmap                  ", 100, 100000, 10, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},

		{"mapWithMutex                 ", 100, 100000, 50, 0x000, 0, &list_head.MapWithLock{}},
		{"sync.Map                     ", 100, 100000, 50, 0x000, 0, syncMap{}},
		{"skiplistmap                  ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap3                 ", 100, 100000, 50, 0x010, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		{"skiplistmap3                 ", 100, 100000, 50, 0x020, skiplistmap.CombineSearch3, newWrapHMap(skiplistmap.NewHMap())},
		{"RMap                         ", 100, 100000, 50, 0x000, 0, newWRMap()},
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
