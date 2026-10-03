package skiplistmap

import "testing"

func BenchmarkPoolInit(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pool := &samepleItemPool[StringKey, any]{}
		pool.Init()
		item, _, lock := pool.Get()
		if lock != nil {
			lock.Unlock()
		}
		if item == nil {
			b.Fatal("Get returned no item")
		}
	}
}
