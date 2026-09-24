package skiplistmap_test

import (
	"strconv"
	"testing"

	"github.com/kazu/skiplistmap"
)

// The code of the stale read and lost update in docs/item-lifetime.md.
func Test_LoadItemAfterPoolGrowth(t *testing.T) {
	for _, p := range []struct {
		name string
		opts []skiplistmap.OptHMap
	}{
		{"default", nil},
		{"embedded pool", []skiplistmap.OptHMap{skiplistmap.UseEmbeddedPool(true)}},
	} {
		t.Run(p.name, func(t *testing.T) {
			m := skiplistmap.New(p.opts...)
			m.Set("a", 1)
			it, _ := m.LoadItem("a")
			for i := 0; i < 100000; i++ {
				m.Set(strconv.Itoa(i), i)
			}
			m.Set("a", 2)
			if got := it.Value(); got != 1 {
				t.Errorf("it.Value() = %v, want the stale 1", got)
			}
			it.SetValue(3)
			if got, _ := m.Get("a"); got != 2 {
				t.Errorf("Get(a) = %v after it.SetValue(3), want 2", got)
			}
		})
	}
}
