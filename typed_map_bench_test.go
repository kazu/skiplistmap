package skiplistmap_test

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

type benchTypedRecord struct {
	Words [32]uint64
	Ref   *uint64
}

func BenchmarkTypedValues(b *testing.B) {
	word := uint64(7)
	record := benchTypedRecord{Words: [32]uint64{7}, Ref: &word}
	b.Run("Scalar", func(b *testing.B) { benchmarkTypedValue(b, word) })
	b.Run("Struct", func(b *testing.B) { benchmarkTypedValue(b, record) })
	b.Run("Pointer", func(b *testing.B) { benchmarkTypedValue(b, &record) })
	b.Run("Slice", func(b *testing.B) { benchmarkTypedValue(b, record.Words[:]) })
	b.Run("Interface", func(b *testing.B) { benchmarkTypedValue[any](b, record) })
}

func benchmarkTypedValue[V any](b *testing.B, value V) {
	for _, embedded := range []bool{false, true} {
		mode := "Pool"
		if embedded {
			mode = "Embedded"
		}
		b.Run(mode, func(b *testing.B) {
			for _, op := range []string{"Get", "Set", "Update", "Keys", "All"} {
				b.Run(op, func(b *testing.B) {
					m := skiplistmap.New[skiplistmap.StringKey, V](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, V](embedded))
					if !m.Set("value", value) {
						b.Fatal("initial Set")
					}
					var before, after runtime.MemStats
					if op == "Set" || op == "Update" {
						runtime.GC()
						runtime.ReadMemStats(&before)
					}
					b.ReportAllocs()
					b.ResetTimer()
					switch op {
					case "Get":
						for i := 0; i < b.N; i++ {
							v, ok := m.Get("value")
							if !ok {
								b.Fatal("Get")
							}
							runtime.KeepAlive(v)
						}
					case "Set":
						for i := 0; i < b.N; i++ {
							if !m.Set("value", value) {
								b.Fatal("Set")
							}
						}
					case "Update":
						for i := 0; i < b.N; i++ {
							if !m.Update("value", func(*V) {}) {
								b.Fatal("Update")
							}
						}
					case "Keys":
						for i := 0; i < b.N; i++ {
							for k := range m.Keys() {
								runtime.KeepAlive(k)
							}
						}
					case "All":
						for i := 0; i < b.N; i++ {
							for k, v := range m.All() {
								runtime.KeepAlive(k)
								runtime.KeepAlive(v)
							}
						}
					}
					b.StopTimer()
					if op == "Set" || op == "Update" {
						runtime.GC()
						runtime.ReadMemStats(&after)
						b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "retained-B/op")
					}
					var entry skiplistmap.Entry[skiplistmap.StringKey, V]
					b.ReportMetric(float64(unsafe.Sizeof(entry)), "entry-B")
					runtime.KeepAlive(m)
				})
			}
		})
	}
}
