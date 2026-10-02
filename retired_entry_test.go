package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	smap "github.com/kazu/skiplistmap"
)

func TestRetiredEntryRefusesReinsertion(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, sameMap := range []bool{false, true} {
			for _, operation := range []string{"purge", "delete", "replace"} {
				t.Run(fmt.Sprintf("embedded=%v/same=%v/%s", embedded, sameMap, operation), func(t *testing.T) {
					a := smap.New[smap.IntKey, int](smap.UseEmbeddedPool[smap.IntKey, int](embedded))
					b := smap.New[smap.IntKey, int](smap.UseEmbeddedPool[smap.IntKey, int](embedded))
					if sameMap {
						b = a
					}
					e := smap.NewEntry[smap.IntKey, int](1, 10)
					defer runtime.KeepAlive(e)
					if !a.StoreItem(e) {
						t.Fatal("initial StoreItem failed")
					}
					switch operation {
					case "purge":
						if !a.Purge(1) {
							t.Fatal("Purge failed")
						}
					case "delete":
						if !a.Delete(1) {
							t.Fatal("Delete failed")
						}
						if err := e.PtrListHead().MarkForDelete(); err != nil {
							t.Fatal(err)
						}
						e.PtrListHead().InitMarked()
					case "replace":
						if !a.Set(1, 20) {
							t.Fatal("Set failed")
						}
						e.PtrListHead().InitMarked()
					}
					beforeA, beforeB := a.Len(), b.Len()
					if b.StoreItem(e) {
						t.Fatal("StoreItem reused a retired entry")
					}
					if a.Len() != beforeA || b.Len() != beforeB {
						t.Fatal("refusal changed Len")
					}
					if operation == "replace" {
						if got, ok := a.Get(1); !ok || got != 20 {
							t.Fatalf("refusal changed existing value: %v, %v", got, ok)
						}
					} else if _, ok := a.Get(1); ok {
						t.Fatal("refusal resurrected the deleted key")
					}
					if !sameMap && b.Len() != 0 {
						t.Fatal("refusal populated the destination")
					}
				})
			}
		}
	}
}
