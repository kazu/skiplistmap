package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"sync"
	"testing"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	slmap "github.com/kazu/skiplistmap"
	"github.com/stretchr/testify/assert"
)

func Test_HmapEntry(t *testing.T) {

	tests := []struct {
		name   string
		wanted slmap.MapItem[string, string]
		got    func(*elist_head.ListHead) slmap.MapItem[string, string]
	}{
		{
			name:   "SampleItem",
			wanted: slmap.NewSampleItem("hoge", "hoge value"),
			got: func(lhead *elist_head.ListHead) slmap.MapItem[string, string] {
				return (slmap.EmptySampleHMapEntry[string, string]()).HmapEntryFromListHead(lhead).(slmap.MapItem[string, string])
			},
		},
		{
			name:   "entryHMap",
			wanted: slmap.NewEntryMap("hogeentry", "hogevalue"),
			got: func(lhead *elist_head.ListHead) slmap.MapItem[string, string] {
				return slmap.EmptyEntryHMap[string, string]().HmapEntryFromListHead(lhead).(slmap.MapItem[string, string])
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

	m := slmap.NewHMap[string, uint64](
		slmap.OptC[string, uint64](
			slmap.BucketMode(slmap.CombineSearch3),
			slmap.MaxPefBucket(32)),
		slmap.UseEmbeddedPool[string, uint64](true))

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

				s := item.(*slmap.SampleItem[string, uint64])
				s.K = "???"
				s.SetValue(i)
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
		{256, 320},
		{320, 384},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.len), func(t *testing.T) {
			assert.Equal(t, tt.want, slmap.PoolCap(tt.len))
		})
	}
}

func Test_HMap(t *testing.T) {

	tests := []struct {
		name string
		m    *WrapHMap[string, *list_head.ListHead]
	}{
		{
			"embedded pool",
			newWrapHMap(slmap.NewHMap[string, *list_head.ListHead](slmap.OptC[string, *list_head.ListHead](slmap.MaxPefBucket(32), slmap.BucketMode(slmap.CombineSearch3)), slmap.UseEmbeddedPool[string, *list_head.ListHead](true))),
		},
		// {
		// 	"pool without goroutine",
		// 	newWrapHMap(slmap.NewHMap(slmap.MaxPefBucket(32), slmap.UsePool(true), slmap.BucketMode(slmap.CombineSearch3))),
		// },
		{
			"combine4",
			newWrapHMap(slmap.NewHMap[string, *list_head.ListHead](slmap.OptC[string, *list_head.ListHead](slmap.MaxPefBucket(32), slmap.BucketMode(slmap.CombineSearch4)), slmap.UsePool[string, *list_head.ListHead](true))),
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
			slmap.ResetStats()

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
					slmap.BucketMode(slmap.NestedSearchForBucket)(m.base.OptC())
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
					slmap.BucketMode(slmap.CombineSearch)(m.base.OptC())
					_, ok = m.Get(fmt.Sprintf("fuge%d", i))
				}
			}

			e := m.base.Last()
			_ = e
			assert.Equal(t, ^uint64(0), e.PtrMapHead().KeyInHmap())
		})

	}
}

func DumpHmap(h *slmap.Map[string, *list_head.ListHead]) {

	fmt.Printf("---DumpBucketPerLevel---\n")
	h.DumpBucketPerLevel(nil)
	fmt.Printf("---DumpBucket---\n")
	h.DumpBucket(nil)
	h.DumpEntry(nil)
	fmt.Printf("fail 0x%x\n", slmap.Failreverse)

}
