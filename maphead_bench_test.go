package skiplistmap

import (
	"testing"
	"unsafe"
)

var recoveredMapHead *MapHead

func BenchmarkMapHeadRecovery(b *testing.B) {
	entries := make([]MapHead, 1024)
	b.Run("Original", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recoveredMapHead = (*MapHead)(ElementOf(unsafe.Pointer(&entries[i%len(entries)].ListHead), mapheadOffset))
		}
	})
	b.Run("List", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			recoveredMapHead = mapheadFromLListHead(&entries[i%len(entries)].ListHead)
		}
	})
}
