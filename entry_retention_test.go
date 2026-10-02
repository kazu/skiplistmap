package skiplistmap

import (
	"runtime"
	"testing"
	"weak"
)

type retentionKey string

func (k retentionKey) KeyHash() (uint64, uint64)     { return 1 << 63, 1 }
func (k retentionKey) Equal(other retentionKey) bool { return k == other }

//go:noinline
func prepareReplacedEntry(t *testing.T) (*Map[retentionKey, int], *Entry[retentionKey, int], weak.Pointer[Entry[retentionKey, int]]) {
	t.Helper()
	m := NewHMap[retentionKey, int]()
	original := NewEntry[retentionKey, int]("target", 0)
	if !m.StoreItem(original) || !m.Set("target", 1) {
		t.Fatal("first replacement failed")
	}
	copy, ok := m.LoadItem("target")
	if !ok {
		t.Fatal("copy not found")
	}
	ref := weak.Make(copy)
	if !m.Set("target", 2) {
		t.Fatal("second replacement failed")
	}
	return m, original, ref
}

//go:noinline
func purgeRetentionCopy(t *testing.T) (*Map[retentionKey, int], weak.Pointer[Entry[retentionKey, int]]) {
	t.Helper()
	m, original, ref := prepareReplacedEntry(t)
	if !m.Purge("target") {
		t.Fatal("purge failed")
	}
	runtime.KeepAlive(original)
	return m, ref
}

func TestEntryRetentionReleasesPurgedCopies(t *testing.T) {
	m, ref := purgeRetentionCopy(t)
	for range 10 {
		runtime.GC()
	}
	if ref.Value() != nil {
		t.Fatal("Map retained a purged copy after the caller released the original")
	}
	runtime.KeepAlive(m)
}

//go:noinline
func expandRetentionPool(t *testing.T, replace bool) (*Map[retentionKey, int], weak.Pointer[Entry[retentionKey, int]]) {
	t.Helper()
	m := NewHMap[retentionKey, int]()
	if !m.Set("anchor", 0) || !m.Set("target", 0) || replace && !m.Set("target", 1) {
		t.Fatal("setup failed")
	}
	pool := samepleItemPoolFromListHead[retentionKey, int](m.pooler.itemPool[poolIndex(1)].Next())
	for range 2 {
		next, err := pool._expand()
		if err != nil {
			t.Fatal(err)
		}
		pool = next
		if !m.Set("anchor", 1) {
			t.Fatal("replacement of the first slot failed")
		}
	}
	if !m.Purge("anchor") {
		t.Fatal("purging the first slot's replacement failed")
	}
	current, ok := m.LoadItem("target")
	if !ok {
		t.Fatal("load after migration failed")
	}
	return m, weak.Make(current)
}

func TestEntryRetentionPoolMigration(t *testing.T) {
	for _, replace := range []bool{false, true} {
		m, ref := expandRetentionPool(t, replace)
		for range 3 {
			runtime.GC()
		}
		if ref.Value() == nil {
			runtime.KeepAlive(m)
			t.Fatalf("registered entry collected after two pool moves, replaced=%v", replace)
		}
		want := 0
		if replace {
			want = 1
		}
		if got, ok := m.Get("target"); !ok || got != want {
			t.Fatalf("Get = %d, %v; want %d, true", got, ok, want)
		}
		for i := 2; i < 5; i++ {
			if !m.Set("target", i) {
				t.Fatal("replacement after migration failed")
			}
			runtime.GC()
			if got, ok := m.Get("target"); !ok || got != i {
				t.Fatalf("Get = %d, %v; want %d, true", got, ok, i)
			}
		}
		runtime.KeepAlive(m)
	}
}
