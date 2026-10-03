package rmap_test

import (
	smap "github.com/kazu/skiplistmap"
	"reflect"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
)

func FuzzOperations(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 3, 2, 1, 0})
	f.Add([]byte{0, 0, 32, 64, 96, 128, 160, 192, 224})
	f.Add([]byte{0, 32, 64, 32, 128, 32, 192, 32, 16, 32})
	f.Add([]byte{0, 64, 128, 192, 32, 16})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 512 {
			ops = ops[:512]
		}
		m := rmap.New[smap.StringKey, any]()
		want := map[string]interface{}{}
		for step, op := range ops {
			key := string([]byte{'a' + op%16})
			switch (op / 16) % 4 {
			case 0:
				var value interface{}
				switch op / 64 {
				case 0:
					value = step
				case 1:
					value = string([]byte{op})
				case 2:
					value = nil
				case 3:
					value = []int{step}
				}
				m.Set(smap.StringKey(key), value)
				want[key] = value
			case 1:
				_, exists := want[key]
				if got := m.Delete(smap.StringKey(key)); got != exists {
					t.Fatalf("step %d: Delete(%q) = %v, want %v", step, key, got, exists)
				}
				delete(want, key)
			case 2:
				got, ok := m.Get(smap.StringKey(key))
				value, exists := want[key]
				if ok != exists || (ok && !reflect.DeepEqual(got, value)) {
					t.Fatalf("step %d: Get(%q) = (%v, %v), want (%v, %v)", step, key, got, ok, value, exists)
				}
			case 3:
				m.Get("missing")
			}
			if got := m.Len(); got != len(want) {
				t.Fatalf("step %d: Len = %d, want %d", step, got, len(want))
			}
		}
		for i := byte(0); i < 16; i++ {
			key := string([]byte{'a' + i})
			got, ok := m.Get(smap.StringKey(key))
			value, exists := want[key]
			if ok != exists || (ok && !reflect.DeepEqual(got, value)) {
				t.Fatalf("final Get(%q) = (%v, %v), want (%v, %v)", key, got, ok, value, exists)
			}
		}
	})
}
