package skiplistmap_test

import (
	"fmt"
	"testing"
)

// BenchmarkMapGet measures Get itself; WrapHMap.Get calls LoadItem instead.
func BenchmarkMapGet(b *testing.B) {
	for _, p := range []crashMapParam{
		{"mode4", func() *WrapHMap { return newPoolMap(32) }},
		{"mode5", func() *WrapHMap { return newEmbeddedMap(32) }},
	} {
		for _, n := range []int{1000, 100000} {
			b.Run(fmt.Sprintf("%s/n=%d", p.name, n), func(b *testing.B) {
				m := p.newMap().base
				keys := make([]string, n)
				for i := range keys {
					keys[i] = fmt.Sprintf("key-%d", i)
					m.Set(keys[i], i)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					j := i % n
					value, ok := m.Get(keys[j])
					if !ok || value != j {
						b.Fatalf("Get(%q)=(%v,%v), want %d", keys[j], value, ok, j)
					}
				}
			})
		}
	}
}
