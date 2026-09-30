//go:build stephook

package skiplistmap_test

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

type comparisonCustomItem struct{ skiplistmap.SampleItem }

func (*comparisonCustomItem) HmapEntryFromListHead(p *elist_head.ListHead) skiplistmap.HMapEntry {
	return (*comparisonCustomItem)(unsafe.Pointer(skiplistmap.SampleItemFromListHead(p)))
}
func (s *comparisonCustomItem) Next() skiplistmap.HMapEntry {
	return s.HmapEntryFromListHead(s.PtrListHead().DirectNext())
}
func (s *comparisonCustomItem) Prev() skiplistmap.HMapEntry {
	return s.HmapEntryFromListHead(s.PtrListHead().DirectPrev())
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
			m := skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(kind == "Sample5"))
			switch kind {
			case "Custom":
				skiplistmap.ItemFn(func() skiplistmap.MapItem { return (*comparisonCustomItem)(nil) })(m)
				e := &comparisonCustomItem{SampleItem: skiplistmap.SampleItem{K: "value"}}
				e.SetValue(0)
				if !m.StoreItem(e) {
					t.Fatal("StoreItem custom")
				}
			case "Entry":
				skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptyEntryHMap })(m)
				if !m.StoreItem(skiplistmap.NewEntryMap("value", 0)) {
					t.Fatal("StoreItem entry")
				}
			default:
				skiplistmap.ItemFn(func() skiplistmap.MapItem { return skiplistmap.EmptySampleHMapEntry })(m)
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
			if _, ok := m.LoadItemByHash(hash, conflict); !ok {
				t.Fatal("LoadItemByHash")
			}
			n := 0
			m.Range(func(k, v interface{}) bool { n++; return true })
			if n != 1 {
				t.Fatalf("Range count: %d", n)
			}
			if !m.Purge("value") || m.Len() != 0 {
				t.Fatal("Purge/Len")
			}
			if kind != "Entry" {
				if len(seen) != 0 {
					t.Fatalf("non-entry entered copy functions: %v", seen)
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
