package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"sync"
	"testing"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
	"github.com/stretchr/testify/assert"
)

func Test_HmapEntry(t *testing.T) {

	tests := []struct {
		name   string
		wanted skiplistmap.MapItem[skiplistmap.StringKey, any]

		got func(*elist_head.ListHead) skiplistmap.MapItem[skiplistmap.StringKey, any]
	}{
		{
			name:   "SampleItem",
			wanted: skiplistmap.NewSampleItem[skiplistmap.StringKey, any]("hoge", "hoge value"),
			got: func(lhead *elist_head.ListHead) skiplistmap.MapItem[skiplistmap.StringKey, any] {
				return (skiplistmap.EmptySampleHMapEntry[skiplistmap.StringKey, any]()).HmapEntryFromListHead(lhead)
			},
		},
		{
			name:   "entryHMap",
			wanted: skiplistmap.NewEntryMap[skiplistmap.StringKey, any]("hogeentry", "hogevalue"),
			got: func(lhead *elist_head.ListHead) skiplistmap.MapItem[skiplistmap.StringKey, any] {
				return (skiplistmap.EmptyEntryHMap[skiplistmap.StringKey, any]()).HmapEntryFromListHead(lhead)
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

	m := skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](32), skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch3))

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

				s := item
				s.InitEntry("???", i)
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

func Test_PoolCap(t *testing.T) {

	tests := []struct {
		len  int
		want int
	}{
		{1, 2},
		{2, 4},
		{3, 4},
		{4, 8},
		{5, 8},
		{8, 16},
		{16, 32},
		{32, 64},
		{64, 128},
		{128, 256},
		{256, 512},
		{320, 512},
		{512, 1024},
		{100000, 131072},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.len), func(t *testing.T) {
			assert.Equal(t, tt.want, skiplistmap.PoolCap(tt.len))
		})
	}
}

func Test_HMap(t *testing.T) {

	tests := []struct {
		name string
		m    *WrapHMap
	}{
		{
			"embedded pool",
			newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](32), skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch3))),
		},
		// {
		// 	"pool without goroutine",
		// 	newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3))),
		// },
		{
			"combine4",
			newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](32), skiplistmap.UsePool[skiplistmap.StringKey, any](true), skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4))),
		},
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
			//DumpHmap(m.base)
			for i := 0; i < 100000; i++ {
				m.Set(fmt.Sprintf("fuge%d", i), v)
			}
			skiplistmap.ResetStats()

			_, success := m.Get("hoge1")
			assert.True(t, success)

			success = m.Delete("hoge3")
			assert.True(t, success)

			_, success = m.Get("1234")
			assert.False(t, success)

			_, success = m.Get("hoge3")
			assert.False(t, success)

			success = m.Set("hoge3", v)
			assert.True(t, success)

			for i := 0; i < 100000; i++ {
				_, ok := m.Get(fmt.Sprintf("fuge%d", i))
				assert.Truef(t, ok, "not found key=%s", fmt.Sprintf("fuge%d", i))
				if !ok {
					skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.NestedSearchForBucket)(m.base)
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
					skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch)(m.base)
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
				}
			}

			var last *skiplistmap.Entry[skiplistmap.StringKey, any]
			m.base.RangeItemForTest(func(e *skiplistmap.Entry[skiplistmap.StringKey, any]) bool {
				last = e
				return true
			})
			value, ok := m.base.Last()
			assert.True(t, ok)
			assert.Equal(t, last.Value(), value)
		})

	}
}

func DumpHmap(h *skiplistmap.Map[skiplistmap.StringKey, any]) {

	fmt.Printf("---DumpBucketPerLevel---\n")
	h.DumpBucketPerLevel(nil)
	fmt.Printf("---DumpBucket---\n")
	h.DumpBucket(nil)
	h.DumpEntry(nil)
	fmt.Printf("fail 0x%x\n", skiplistmap.Failreverse)

}
