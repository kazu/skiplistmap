//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

type comparisonCustomItem struct {
	skiplistmap.SampleItem[skiplistmap.StringKey, any]
}

func TestEntryCopyRouteIsolation(t *testing.T) {
	for _, kind := range []string{"Sample4", "Sample5", "Custom", "Entry"} {
		t.Run(kind, func(t *testing.T) {
			seen := make(map[string]int)
			skiplistmap.SetStepHook(func(point string, _, _ unsafe.Pointer) {
				if strings.HasPrefix(point, "copy.") {
					seen[point]++
				}
			})
			defer skiplistmap.SetStepHook(nil)
			m := skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](kind == "Sample5"))
			var root *skiplistmap.Entry[skiplistmap.StringKey, any]
			switch kind {
			case "Custom":

				e := &comparisonCustomItem{}
				e.InitEntry("value", 0)
				root = &e.SampleItem
				if !m.StoreItem(root) {
					t.Fatal("StoreItem custom")
				}
			case "Entry":

				root = skiplistmap.NewEntryMap[skiplistmap.StringKey, any]("value", 0)
				if !m.StoreItem(root) {
					t.Fatal("StoreItem entry")
				}
			default:

				if !m.Set("value", 0) {
					t.Fatal("initial Set")
				}
			}
			if !m.Set("value", 1) {
				t.Fatal("update")
			}
			if v, ok := m.Get("value"); !ok || v != 1 {
				t.Fatalf("Get: %v %v", v, ok)
			}
			hash, conflict := skiplistmap.KeyToHash("value")
			if value, ok := m.GetByHash(hash, conflict); !ok || value != 1 {
				t.Fatalf("GetByHash: %v %v", value, ok)
			}
			n := 0
			m.Range(func(k skiplistmap.StringKey, v any) bool { n++; return true })
			if n != 1 {
				t.Fatalf("Range count: %d", n)
			}
			if !m.Purge("value") || m.Len() != 0 {
				t.Fatal("Purge/Len")
			}
			runtime.KeepAlive(root)
			if kind == "Sample5" {
				if seen["copy.update"] != 0 || seen["copy.delete"] != 0 {
					t.Fatalf("embedded update entered nonembedded publication: %v", seen)
				}
			} else {
				for _, point := range []string{"copy.search", "copy.match", "copy.update", "copy.range", "copy.delete"} {
					if seen[point] == 0 {
						t.Errorf("positive control missed %s", point)
					}
				}
			}
		})
	}
}
