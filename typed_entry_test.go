package skiplistmap

import (
	"fmt"
	"sync"
	"testing"
)

type reuseValue struct {
	Words   [64]uint64
	Text    string
	Pointer *uint64
}

func TestTypedEmbeddedUpdatesReuseSlots(t *testing.T) {
	m := New[Uint64Key, int](UseEmbeddedPool[Uint64Key, int](true))
	for i := 0; i < 64; i++ {
		if !m.Set(1, i) {
			t.Fatal("Set failed")
		}
	}
	if value, ok := m.Get(1); !ok || value != 63 {
		t.Fatalf("Get=(%d,%v)", value, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("Len=%d", m.Len())
	}
	pool := m.findBucket(uint64(1) << 63).itemPool()
	if got := pool.len(); got > 2 {
		t.Fatalf("single-key updates retained %d slots; retired slots were not reused", got)
	}
}

func makeReuseValue(n uint64) reuseValue {
	v := reuseValue{Text: fmt.Sprint(n), Pointer: &n}
	for i := range v.Words {
		v.Words[i] = n
	}
	return v
}

func TestTypedEntryReuseSnapshot(t *testing.T) {
	e := NewEntry(Uint64Key(0), makeReuseValue(0))
	e.reusable = true
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for n := uint64(1); n < 2000; n++ {
			e.storeTypedKeyValue(Uint64Key(n), makeReuseValue(n))
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for n := 0; n < 2000; n++ {
			key, value, _ := e.loadTypedKeyValue()
			for _, word := range value.Words {
				if word != uint64(key) {
					t.Errorf("torn snapshot: key=%d word=%d", key, word)
					return
				}
			}
			if *value.Pointer != uint64(key) || value.Text != fmt.Sprint(uint64(key)) {
				t.Errorf("pointer/string from another publication: key=%d value=%+v", key, value)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}
