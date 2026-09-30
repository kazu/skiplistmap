package skiplistmap_test

import (
	"github.com/kazu/skiplistmap"
	"testing"
)

func BenchmarkEntryCopyOtherRoutes(b *testing.B) {
	for _, kind := range []string{"Sample4", "Sample5"} {
		b.Run(kind, func(b *testing.B) {
			for _, op := range []string{"Set", "Get"} {
				b.Run(op, func(b *testing.B) {
					m := skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(kind == "Sample5"))
					skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptySampleHMapEntry })(m)
					if !m.Set("value", 1) {
						b.Fatal("initial Set")
					}
					b.ReportAllocs()
					b.ResetTimer()
					if op == "Set" {
						for i := 0; i < b.N; i++ {
							if !m.Set("value", 1) {
								b.Fatal("Set")
							}
						}
					} else {
						for i := 0; i < b.N; i++ {
							if v, ok := m.Get("value"); !ok || v != 1 {
								b.Fatal("Get")
							}
						}
					}
					b.StopTimer()
					if m.Len() != 1 {
						b.Fatal("Len")
					}
				})
			}
		})
	}
}
