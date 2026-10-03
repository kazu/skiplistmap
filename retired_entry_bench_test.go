package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	smap "github.com/kazu/skiplistmap"
)

func BenchmarkExternalStorePurge(b *testing.B) {
	for _, embedded := range []bool{false, true} {
		b.Run(fmt.Sprint(embedded), func(b *testing.B) {
			b.ReportAllocs()
			for done := 0; done < b.N; {
				b.StopTimer()
				m := smap.New[smap.Uint64Key, int](smap.UseEmbeddedPool[smap.Uint64Key, int](embedded))
				entries := make([]smap.Entry[smap.Uint64Key, int], min(1024, b.N-done))
				for i := range entries {
					entries[i].InitEntry(smap.Uint64Key(i), i)
				}
				b.StartTimer()
				for i := range entries {
					if !m.StoreItem(&entries[i]) || !m.Purge(smap.Uint64Key(i)) {
						b.Fatal("StoreItem/Purge failed")
					}
				}
				done += len(entries)
				runtime.KeepAlive(entries)
			}
		})
	}
}
