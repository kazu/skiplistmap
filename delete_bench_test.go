package skiplistmap_test

import (
	"runtime"
	"testing"

	smap "github.com/kazu/skiplistmap"
)

// Setup is outside the timer so these cases measure deletion, not reinsertion.
func BenchmarkDeleteMembership(b *testing.B) {
	for _, source := range []string{"caller", "pool", "embedded"} {
		for _, operation := range []string{"Delete", "Purge"} {
			b.Run(source+"/"+operation, func(b *testing.B) {
				const size = 1024
				b.ReportAllocs()
				for done := 0; done < b.N; {
					b.StopTimer()
					m := smap.New[smap.Uint64Key, int](smap.UseEmbeddedPool[smap.Uint64Key, int](source == "embedded"))
					entries := make([]smap.Entry[smap.Uint64Key, int], size)
					for i := range entries {
						key := smap.Uint64Key(i)
						if source == "caller" {
							entries[i].InitEntry(key, i)
							if !m.StoreItem(&entries[i]) {
								b.Fatal("StoreItem failed")
							}
						} else if !m.Set(key, i) {
							b.Fatal("Set failed")
						}
					}
					n := min(size, b.N-done)
					b.StartTimer()
					if operation == "Delete" {
						for i := 0; i < n; i++ {
							if !m.Delete(smap.Uint64Key(i)) {
								b.Fatal("Delete failed")
							}
						}
					} else {
						for i := 0; i < n; i++ {
							if !m.Purge(smap.Uint64Key(i)) {
								b.Fatal("Purge failed")
							}
						}
					}
					done += n
					runtime.KeepAlive(entries)
				}
			})
		}
	}
}
