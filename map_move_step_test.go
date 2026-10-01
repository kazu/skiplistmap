//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Purge and Set of the same key, again and again in one goroutine, make the
// item pools grow. After each round every entry of the list must lie in the
// array of a pool: an entry left in an array that the pool replaced is freed
// by the GC, since the list holds it by offsets only.
func Test_PurgeAndSetKeepsEntriesInThePools(t *testing.T) {
	const present = 1000
	const rounds = 60
	m := newPoolMap(32)
	prefill(t, m, present)
	for r := 0; r < rounds; r++ {
		for k := 0; k < present; k++ {
			m.Delete(crashKey(k))
			if !m.Set(crashKey(k), &list_head.ListHead{}) {
				t.Fatalf("round %d: Set(%q) = false", r, crashKey(k))
			}
		}
		if err := skiplistmap.StepCheckPooledItems[skiplistmap.StringKey, any](m.base); err != nil {
			t.Fatalf("round %d: %v", r, err)
		}
		if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](m.base); err != nil {
			t.Fatalf("round %d: %v", r, err)
		}
		runtime.GC()
		for k := 0; k < present; k++ {
			if _, ok := m.Get(crashKey(k)); !ok {
				t.Fatalf("round %d: Get(%q) not found", r, crashKey(k))
			}
		}
	}
}
