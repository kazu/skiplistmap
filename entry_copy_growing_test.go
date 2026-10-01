package skiplistmap_test

import (
	"runtime"
	"sync"
	"testing"

	"github.com/kazu/skiplistmap"
)

func TestEntryCopyWhileMixedPoolGrows(t *testing.T) {
	m := skiplistmap.New[skiplistmap.IntKey, int](
		skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](true),
		skiplistmap.MaxPefBucket[skiplistmap.IntKey, int](8))
	entry := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 0)
	defer runtime.KeepAlive(entry)
	if !m.StoreItem(entry) || !m.Set(2, 0) {
		t.Fatal("setup")
	}
	var wg sync.WaitGroup
	for _, key := range []skiplistmap.IntKey{1, 2} {
		wg.Add(1)
		go func(key skiplistmap.IntKey) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if !m.Set(key, i) {
					t.Error("Set existing key")
					return
				}
			}
		}(key)
	}
	for key := skiplistmap.IntKey(3); key < 131; key++ {
		if !m.Set(key, int(key)) {
			t.Error("growing Set")
			break
		}
	}
	wg.Wait()
	for _, key := range []skiplistmap.IntKey{1, 2} {
		if v, ok := m.Get(key); !ok || v != 499 {
			t.Fatalf("Get(%d) = %d, %v", key, v, ok)
		}
	}
}
