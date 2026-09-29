package skiplistmap_test

import (
	"fmt"
	"testing"

	"github.com/kazu/skiplistmap"
)

func BenchmarkEmbeddedDeleteReinsert(b *testing.B) {
	for _, n := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			m := skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(32))
			keys := make([]string, n)
			for i := range keys {
				keys[i] = crashKey(i)
				if !m.Set(keys[i], i) {
					b.Fatal("initial Set failed")
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[i%n]
				if !m.Delete(key) || !m.Set(key, i) {
					b.Fatal("Delete or Set failed")
				}
			}
		})
	}
}
