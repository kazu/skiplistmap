package typedapi

import (
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

var (
	benchHash  uint64
	benchEntry *Entry[StringKey, user]
)

func hashGeneric[K Key[K]](key K) (uint64, uint64) { return key.KeyHash() }

func BenchmarkKeyHash(b *testing.B) {
	key := "example-key"
	b.Run("existing", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hash, conflict := skiplistmap.KeyToHash(key)
			benchHash = hash ^ conflict
		}
	})
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hash, conflict := hashGeneric(StringKey(key))
			benchHash = hash ^ conflict
		}
	})
}

func BenchmarkEntryRecovery(b *testing.B) {
	e := NewEntry(StringKey("a"), user{Name: "alice", Age: 20})
	view := NewEntryView[StringKey, user]()
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(unsafe.Sizeof(*e)), "entry-bytes")
	for i := 0; i < b.N; i++ {
		benchEntry = view.Element(&e.ListHead)
	}
}

func BenchmarkRecoverField(b *testing.B) {
	e := NewEntry(StringKey("a"), user{Name: "alice", Age: 20})
	view := NewEntryView[StringKey, user]()
	field := &e.value.Age
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchEntry = view.RecoverField(field, unsafe.Offsetof(user{}.Age))
	}
}
