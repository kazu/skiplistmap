package rmap_test

import (
	smap "github.com/kazu/skiplistmap"
	"reflect"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
)

func TestValueTypeChanges(t *testing.T) {
	values := []interface{}{1, 2, "one", "two", nil, nil,
		[]int{1}, []int{2}, map[string]int{"x": 1}, map[string]int{"x": 2}, (*int)(nil)}
	for _, promote := range []bool{false, true} {
		for i := 1; i < len(values); i++ {
			m := rmap.New[smap.StringKey, any]()
			m.Set("key", values[i-1])
			if promote {
				m.Get("key")
			}
			m.Set("key", values[i])
			got, ok := m.Get("key")
			if !ok || !reflect.DeepEqual(got, values[i]) || m.Len() != 1 {
				t.Fatalf("promoted %v, value %d: Get = (%v, %v), Len = %d", promote, i, got, ok, m.Len())
			}
			if !m.Delete("key") || m.Len() != 0 {
				t.Fatalf("promoted %v, value %d: Delete or Len failed", promote, i)
			}
			m.Set("key", values[i])
			got, ok = m.Get("key")
			if !ok || !reflect.DeepEqual(got, values[i]) || m.Len() != 1 {
				t.Fatalf("promoted %v, value %d: reinsertion = (%v, %v), Len = %d", promote, i, got, ok, m.Len())
			}
		}
	}
}
