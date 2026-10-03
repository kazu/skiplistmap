//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

func TestEntryValueEndpointSlotReuse(t *testing.T) {
	for _, last := range []bool{false, true} {
		t.Run(fmt.Sprint(last), func(t *testing.T) {
			m := skiplistmap.New[fixedHashKey, int](
				skiplistmap.UseEmbeddedPool[fixedHashKey, int](true),
				skiplistmap.MaxPefBucket[fixedHashKey, int](16))
			keys := []fixedHashKey{
				{"old", bits.Reverse64(2<<60 + 1), 1},
				{"remaining", bits.Reverse64(3 << 60), 1},
				{"reused", bits.Reverse64(2<<60 + 2), 1},
				{"new-end", bits.Reverse64(1 << 60), 1},
			}
			if last {
				keys[1], keys[3] = keys[3], keys[1]
			}
			m.Set(keys[0], 10)
			m.Set(keys[1], 20)
			old, _ := m.LoadItemForTest(keys[0])
			s := newStepper(t)
			stop := s.stopAt("map.end.selected", isNode(unsafe.Pointer(old.PtrListHead())))
			var value int
			var found bool
			done := goStep(t, func() {
				if last {
					value, found = m.Last()
				} else {
					value, found = m.First()
				}
			})
			stop.waitReached(t, done)
			if !m.Purge(keys[0]) || !m.Set(keys[3], 40) || !m.Set(keys[2], 30) {
				t.Fatal("replacement failed")
			}
			reused, _ := m.LoadItemForTest(keys[2])
			if reused != old {
				t.Fatal("test did not reuse the selected slot")
			}
			stop.Release()
			waitDone(t, done, "endpoint reuse")
			if found || value != 0 {
				t.Fatalf("endpoint=(%d,%v), want (0,false) after slot reuse", value, found)
			}
		})
	}
}

func TestSearchIntermediatePurgedAfterMarkCheck(t *testing.T) {
	m := newHashStepMap()
	keys := []fixedHashKey{
		{"first", bits.Reverse64(1 << 60), 1},
		{"middle", bits.Reverse64(1<<60 + 1), 1},
		{"target", bits.Reverse64(1<<60 + 2), 1},
	}
	for i, key := range keys {
		if !m.Set(key, i+10) {
			t.Fatal("initial Set failed")
		}
	}
	if value, found := m.Get(keys[2]); !found || value != 12 {
		t.Fatal("target was not present before traversal")
	}
	middle, _ := m.LoadItem(keys[1])
	s := newStepper(t)
	stop := s.stopAt("map.search.unmarked", isNode(unsafe.Pointer(middle.PtrListHead())))
	var value any
	var found bool
	done := goStep(t, func() { value, found = m.Get(keys[2]) })
	stop.waitReached(t, done)
	if !m.Purge(keys[1]) {
		t.Fatal("Purge failed")
	}
	stop.Release()
	waitDone(t, done, "search past purged intermediate")
	if found || value != nil {
		t.Fatalf("Get=(%v,%v), want failure after losing the traversal cursor", value, found)
	}
	if value, found := m.Get(keys[2]); !found || value != 12 {
		t.Fatal("target did not remain present after traversal")
	}
}

func TestEntryValueDuringUpdate(t *testing.T) {
	for _, api := range []string{"First", "Last", "SearchKey"} {
		for _, operation := range []string{"replace", "purge", "reuse"} {
			for _, point := range []string{"map.get.beforeValue", "map.key.dataRead"} {
				t.Run(fmt.Sprintf("%s/%s/%s", api, operation, point), func(t *testing.T) {
					m := skiplistmap.New[skiplistmap.StringKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, int](true))
					if operation == "replace" {
						// Grow first so replacement retires the observed slot in place.
						m.Set("a", 9)
					}
					m.Set("a", 10)
					old, _ := m.LoadItemForTest("a")
					s := newStepper(t)
					stop := s.stopAt(point, isNode(unsafe.Pointer(old.PtrListHead())))
					var value int
					var found bool
					done := goStep(t, func() {
						switch api {
						case "First":
							value, found = m.First()
						case "Last":
							value, found = m.Last()
						case "SearchKey":
							hash, _ := skiplistmap.StringKey("a").KeyHash()
							value, found = m.SearchKey(hash)
						}
					})
					stop.waitReached(t, done)
					if operation != "replace" && !m.Purge("a") {
						t.Fatal("Purge failed")
					}
					if operation != "purge" && !m.Set("a", 20) {
						t.Fatal("Set failed")
					}
					if operation == "reuse" {
						replacement, _ := m.LoadItemForTest("a")
						if replacement != old {
							t.Fatal("test did not reuse the old slot")
						}
					}
					stop.Release()
					waitDone(t, done, api)
					if operation == "purge" {
						if found {
							t.Fatalf("purged value returned: %d", value)
						}
					} else if found && value != 20 || !found && value != 0 {
						t.Fatalf("value=(%d,%v), want current value or failure", value, found)
					}
				})
			}
		}
	}
}
