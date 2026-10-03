package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

func BenchmarkMixedEntries(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		for _, operation := range []string{"Get", "Set"} {
			b.Run(fmt.Sprintf("%d/%s", count, operation), func(b *testing.B) {
				var empty, populated, updated runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&empty)
				m := skiplistmap.New[skiplistmap.StringKey, int](
					skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, int](true))
				entries := make([]skiplistmap.Entry[skiplistmap.StringKey, int], count/2)
				keys := make([]skiplistmap.StringKey, count)
				for i := range keys {
					keys[i] = skiplistmap.StringKey(fmt.Sprintf("%d", i))
					if i%2 == 0 {
						entries[i/2].InitEntry(keys[i], i)
						if !m.StoreItem(&entries[i/2]) {
							b.Fatal("external Entry registration failed")
						}
					} else if !m.Set(keys[i], i) {
						b.Fatal("pool insertion failed")
					}
				}
				for i, key := range keys {
					if value, ok := m.Get(key); !ok || value != i {
						b.Fatalf("initial Get(%q) = (%d, %v)", key, value, ok)
					}
				}
				runtime.GC()
				runtime.ReadMemStats(&populated)
				b.ReportAllocs()
				b.ResetTimer()
				if operation == "Get" {
					for i := 0; i < b.N; i++ {
						index := i % count
						if value, ok := m.Get(keys[index]); !ok || value != index {
							b.Fatal("mixed Get failed")
						}
					}
				} else {
					for i := 0; i < b.N; i++ {
						if !m.Set(keys[i%count], i) {
							b.Fatal("mixed Set failed")
						}
					}
				}
				b.StopTimer()
				runtime.GC()
				runtime.ReadMemStats(&updated)
				b.ReportMetric(float64(int64(populated.HeapAlloc)-int64(empty.HeapAlloc))/float64(count), "live-B/key")
				b.ReportMetric(float64(int64(updated.HeapAlloc)-int64(populated.HeapAlloc))/float64(b.N), "retained-B/op")
				for i, key := range keys {
					want := i
					if operation == "Set" && i < b.N {
						want = i + (b.N-1-i)/count*count
					}
					if value, ok := m.Get(key); !ok || value != want {
						b.Fatalf("final Get(%q) = (%d, %v), want %d", key, value, ok, want)
					}
				}
				runtime.KeepAlive(m)
				runtime.KeepAlive(entries)
			})
		}
	}
}
