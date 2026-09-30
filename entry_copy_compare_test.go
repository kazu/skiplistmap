package skiplistmap_test

import (
	"reflect"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

func newEntryCopyMap(t testing.TB) (*skiplistmap.Map, skiplistmap.MapItem) {
	t.Helper()
	m := skiplistmap.NewHMap()
	skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptyEntryHMap })(m)
	e := skiplistmap.NewEntryMap("value", 0)
	if !m.StoreItem(e) {
		t.Fatal("StoreItem failed")
	}
	return m, e
}

func TestEntryCopyCallerReference(t *testing.T) {
	m, e := newEntryCopyMap(t)
	if !m.Set("value", 1) {
		t.Fatal("Set failed")
	}
	if got := e.Value(); got != 0 {
		t.Fatalf("replaced entry value = %v, want original 0", got)
	}
	if !e.PtrMapHead().IsDeleted() {
		t.Fatal("replaced entry still reports membership")
	}
	if got, ok := m.Get("value"); !ok || got != 1 {
		t.Fatalf("current value = %v, %v; want 1, true", got, ok)
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyGC(t *testing.T) {
	m, e := newEntryCopyMap(t)
	for i := 1; i <= 100; i++ {
		if !m.Set("value", i) {
			t.Fatal("Set failed")
		}
		runtime.GC()
		got, ok := m.Get("value")
		if !ok || got != i {
			t.Fatalf("Get after GC = %v, %v; want %d, true", got, ok, i)
		}
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyDirectSetValue(t *testing.T) {
	m, e := newEntryCopyMap(t)
	if e.SetValue(1) {
		t.Fatal("linked entry allowed direct mutation")
	}
	if got, ok := m.Get("value"); !ok || got != 0 {
		t.Fatalf("Get = %v, %v; want 0, true", got, ok)
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyValues(t *testing.T) {
	m, e := newEntryCopyMap(t)
	pointer := new(int)
	for _, value := range []interface{}{nil, 7, "text", pointer, nil} {
		if !m.Set("value", value) {
			t.Fatal("Set failed")
		}
		if got, ok := m.Get("value"); !ok || got != value {
			t.Fatalf("Get = %v, %v; want %v, true", got, ok, value)
		}
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyRetainedHeap(t *testing.T) {
	m, e := newEntryCopyMap(t)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 100000; i++ {
		if !m.Set("value", 1) {
			t.Fatal("Set failed")
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	example := skiplistmap.NewEntryMap("size", 0)
	t.Logf("100000 updates: retained heap delta=%d bytes; entry size=%d; GC cycles=%d",
		int64(after.HeapAlloc)-int64(before.HeapAlloc), unsafe.Sizeof(*example), after.NumGC-before.NumGC)
	runtime.KeepAlive(e)
	runtime.KeepAlive(m)
}

func TestEntryCopyCollisionRange(t *testing.T) {
	t.Skip("baseline 54c7318 also loses Get(int) after StoreItem; non-string entry hash compatibility is not changed by copy publication")
	testEntryCopyRange(t, []interface{}{int(1), uint64(1)})
}

func TestEntryCopyRange(t *testing.T) {
	testEntryCopyRange(t, []interface{}{"left", "right"})
}

func TestEntryCopyAdjacentChurn(t *testing.T) {
	m, root := newEntryCopyMap(t)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if !m.Set("value", i) {
				t.Error("Set failed")
				return
			}
			if _, ok := m.Get("value"); !ok {
				t.Error("anchor key disappeared")
				return
			}
		}
	}()
	for _, key := range []string{"neighbor-left", "neighbor-right"} {
		go func(key string) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				e := skiplistmap.NewEntryMap(key, i)
				if !m.StoreItem(e) {
					t.Error("neighbor StoreItem failed")
					return
				}
				if !m.Purge(key) {
					t.Error("neighbor Purge failed")
					return
				}
				runtime.KeepAlive(e)
			}
		}(key)
	}
	wg.Wait()
	if m.Len() != 1 {
		t.Fatalf("Len=%d", m.Len())
	}
	if _, ok := m.Get("value"); !ok {
		t.Fatal("anchor missing after churn")
	}
	runtime.KeepAlive(root)
}

func testEntryCopyRange(t *testing.T, keys []interface{}) {
	m := skiplistmap.NewHMap()
	skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptyEntryHMap })(m)
	var roots []skiplistmap.MapItem
	for _, key := range keys {
		e := skiplistmap.NewEntryMap(key, key)
		roots = append(roots, e)
		if !m.StoreItem(e) {
			t.Fatal("StoreItem failed")
		}
	}
	for i := 0; i < 100; i++ {
		for _, key := range keys {
			if !m.StoreItem(skiplistmap.NewEntryMap(key, key)) {
				t.Fatal("StoreItem update failed")
			}
			if got, ok := m.Get(key); !ok || got != key {
				t.Fatalf("collision Get(%T) = %v, %v", key, got, ok)
			}
		}
		seen := make(map[interface{}]bool)
		m.Range(func(key, value interface{}) bool {
			if seen[key] || key != value {
				t.Errorf("Range duplicate or mismatched key/value: %T %v, %T %v", key, key, value, value)
			}
			seen[key] = true
			return true
		})
		if len(seen) != 2 || m.Len() != 2 {
			t.Fatalf("Range count=%d Len=%d", len(seen), m.Len())
		}
	}
	runtime.KeepAlive(roots)
}

func TestEntryCopyContendedUpdate(t *testing.T) {
	m, e := newEntryCopyMap(t)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				if !m.Set("value", i) {
					t.Error("Set failed")
					return
				}
				if _, ok := m.Get("value"); !ok {
					t.Error("continuously present key disappeared")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if !m.Set("value", 4000) {
		t.Fatal("final Set failed")
	}
	if got, ok := m.Get("value"); !ok || got != 4000 {
		t.Fatalf("final Get = %v, %v", got, ok)
	}
	if n := m.Len(); n != 1 {
		t.Fatalf("Len = %v, want 1", n)
	}
	checkEntryCopyMap(t, m, e)
	runtime.KeepAlive(e)
}

func checkEntryCopyMap(t testing.TB, m *skiplistmap.Map, e skiplistmap.MapItem) {
	t.Helper()
	count := 0
	m.RangeItem(func(item skiplistmap.MapItem) bool {
		count++
		if reflect.TypeOf(item) != reflect.TypeOf(e) {
			t.Errorf("entry type changed from %T to %T", e, item)
		}
		return count < 3
	})
	if count != 1 || m.Len() != 1 {
		t.Errorf("final RangeItem count=%d Len=%d; want 1", count, m.Len())
	}
}

func BenchmarkEntryCopyCompare(b *testing.B) {
	for _, name := range []string{"Set", "Get", "Mixed", "Contended"} {
		b.Run(name, func(b *testing.B) {
			m, e := newEntryCopyMap(b)
			b.ReportAllocs()
			b.ResetTimer()
			if name == "Contended" {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if !m.Set("value", 1) {
							b.Error("Set failed")
							return
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					if name == "Set" || (name == "Mixed" && i%8 == 0) {
						if !m.Set("value", 1) {
							b.Fatal("Set failed")
						}
					} else if _, ok := m.Get("value"); !ok {
						b.Fatal("Get failed")
					}
				}
			}
			b.StopTimer()
			checkEntryCopyMap(b, m, e)
			runtime.KeepAlive(e)
		})
	}
}
