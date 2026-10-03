package rmap_test

import (
	smap "github.com/kazu/skiplistmap"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
)

func BenchmarkReadDeleteReinsert(b *testing.B) {
	m := rmap.New[smap.StringKey, int]()
	m.Set("key", 1)
	m.Get("key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !m.Delete("key") || !m.Set("key", i) || m.Len() != 1 {
			b.Fatal("Delete, Set, or Len failed")
		}
	}
	b.StopTimer()
	if got, ok := m.Get("key"); !ok || got != b.N-1 {
		b.Fatalf("final value = (%v, %v)", got, ok)
	}
}
