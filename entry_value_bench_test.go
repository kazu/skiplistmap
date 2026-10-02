package skiplistmap_test

import (
	"fmt"
	"testing"

	"github.com/kazu/skiplistmap"
)

func BenchmarkEntryValueRead(b *testing.B) {
	for _, embedded := range []bool{false, true} {
		m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
		m.Set(1, 10)
		hash, conflict := skiplistmap.IntKey(1).KeyHash()
		for _, tc := range []struct {
			name string
			read func() (int, bool)
		}{
			{"First", m.First},
			{"Last", m.Last},
			{"SearchKey", func() (int, bool) { return m.SearchKey(hash) }},
			{"GetByHash", func() (int, bool) { return m.GetByHash(hash, conflict) }},
		} {
			b.Run(fmt.Sprintf("embedded=%v/%s", embedded, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if value, ok := tc.read(); !ok || value != 10 {
						b.Fatalf("value=(%d,%v), want (10,true)", value, ok)
					}
				}
			})
		}
	}
}
