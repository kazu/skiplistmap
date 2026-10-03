package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

func BenchmarkUpdateExternal(b *testing.B) {
	for _, embedded := range []bool{false, true} {
		for _, operation := range []string{"Set", "Update"} {
			b.Run(fmt.Sprintf("embedded=%v/%s", embedded, operation), func(b *testing.B) {
				m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
				entry := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 7)
				if !m.StoreItem(entry) {
					b.Fatal("StoreItem")
				}
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				b.ReportAllocs()
				b.ResetTimer()
				if operation == "Set" {
					for range b.N {
						if !m.Set(1, 7) {
							b.Fatal("Set")
						}
					}
				} else {
					for range b.N {
						if !m.Update(1, func(*int) {}) {
							b.Fatal("Update")
						}
					}
				}
				b.StopTimer()
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "retained-B/op")
				runtime.KeepAlive(entry)
				runtime.KeepAlive(m)
			})
		}
	}
}
