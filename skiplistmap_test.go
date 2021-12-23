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
		wanted skiplistmap.MapItem
		got    func(*elist_head.ListHead) skiplistmap.MapItem
	}{
		{
			name:   "SampleItem",
			wanted: skiplistmap.NewSampleItem("hoge", "hoge value"),
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

func Test_HMap(t *testing.T) {

	tests := []struct {
		name string
		m    *WrapHMap
	}{
		// {
		// 	"embedded pool",
		// 	newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UseEmbeddedPool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3))),
		// },
		// {
		// 	"pool without goroutine",
		// 	newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch3))),
		// },
		{
			"combine4",
			newWrapHMap(skiplistmap.NewHMap(skiplistmap.MaxPefBucket(32), skiplistmap.UsePool(true), skiplistmap.BucketMode(skiplistmap.CombineSearch4))),
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
