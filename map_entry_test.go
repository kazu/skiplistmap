package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

func Test_EntryMapConcurrentValue(t *testing.T) {
	m := skiplistmap.NewHMap[skiplistmap.StringKey, any]()

	e := skiplistmap.NewEntryMap[skiplistmap.StringKey, any]("value", 0)
	if !m.StoreItem(e) {
		t.Fatal("initial StoreItem failed")
	}
	runTogether(2, func(g int) {
		for i := 0; i < 1000; i++ {
			if g == 0 {
				m.Set("value", i)
			} else if _, ok := m.Get("value"); !ok {
				t.Error("existing key was lost")
			}
		}
	})
	for _, value := range []interface{}{nil, "changed type", 7} {
		if !m.Set("value", value) {
			t.Fatal("Set failed")
		}
		if got, ok := m.Get("value"); !ok || got != value {
			t.Fatalf("Get = (%v, %v), want (%v, true)", got, ok, value)
		}
	}
	runtime.KeepAlive(e)
}

func BenchmarkEntryMapSet(b *testing.B) {
	m := skiplistmap.NewHMap[skiplistmap.StringKey, any]()

	e := skiplistmap.NewEntryMap[skiplistmap.StringKey, any]("value", 0)
	if !m.StoreItem(e) {
		b.Fatal("initial StoreItem failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !m.Set("value", i) {
			b.Fatal("Set failed")
		}
	}
	runtime.KeepAlive(e)
}
