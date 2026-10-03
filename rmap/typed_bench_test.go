package rmap_test

import (
	smap "github.com/kazu/skiplistmap"
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
)

func benchmarkTypedValue[V any](b *testing.B, value V) {
	for _, promoted := range []bool{false, true} {
		phase := "dirty"
		if promoted {
			phase = "read"
		}
		b.Run(phase, func(b *testing.B) {
			for _, operation := range []string{"get", "set"} {
				if !promoted && operation == "get" {
					continue // Get promotes this one-key map on its first miss.
				}
				b.Run(operation, func(b *testing.B) {
					m := rmap.New[smap.StringKey, V]()
					if !m.Set("key", value) {
						b.Fatal("initial Set failed")
					}
					if promoted {
						m.Get("key")
					}
					b.ReportAllocs()
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					b.ResetTimer()
					if operation == "get" {
						for i := 0; i < b.N; i++ {
							if _, ok := m.Get("key"); !ok {
								b.Fatal("Get failed")
							}
						}
					} else {
						for i := 0; i < b.N; i++ {
							if !m.Set("key", value) {
								b.Fatal("Set failed")
							}
						}
					}
					b.StopTimer()
					runtime.GC()
					runtime.ReadMemStats(&after)
					b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "retained-B/op")
					runtime.KeepAlive(m)
					if m.Len() != 1 {
						b.Fatal("Len changed")
					}
				})
			}
		})
	}
}

func BenchmarkTypedValue(b *testing.B) {
	b.Run("int", func(b *testing.B) { benchmarkTypedValue(b, 1000) })
	b.Run("string", func(b *testing.B) { benchmarkTypedValue(b, "value") })
	b.Run("struct128", func(b *testing.B) { benchmarkTypedValue(b, [16]uint64{1}) })
	b.Run("pointer", func(b *testing.B) { benchmarkTypedValue(b, new(int)) })
	b.Run("any-nil", func(b *testing.B) { benchmarkTypedValue[any](b, nil) })
}
