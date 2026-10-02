package skiplistmap_test

import (
	"runtime"
	"testing"
	"weak"

	skiplistmap "github.com/kazu/skiplistmap"
)

type registeredLifetimeKey string

func (k registeredLifetimeKey) KeyHash() (uint64, uint64)              { return 1 << 63, 1 }
func (k registeredLifetimeKey) Equal(other registeredLifetimeKey) bool { return k == other }

// Return only a weak reference to the current entry so the test itself does
// not retain an entry that the map is responsible for keeping registered.
//
//go:noinline
func prepareRegisteredEntry(t *testing.T, callerOwned bool, updates int, useUpdate ...bool) (*skiplistmap.Map[registeredLifetimeKey, int], *skiplistmap.Entry[registeredLifetimeKey, int], weak.Pointer[skiplistmap.Entry[registeredLifetimeKey, int]]) {
	t.Helper()
	m := skiplistmap.NewHMap[registeredLifetimeKey, int]()
	var original *skiplistmap.Entry[registeredLifetimeKey, int]
	if callerOwned {
		original = skiplistmap.NewEntry[registeredLifetimeKey, int]("target", 0)
		if !m.StoreItem(original) {
			t.Fatal("StoreItem failed")
		}
	} else if !m.Set("target", 0) {
		t.Fatal("initial Set failed")
	}
	for value := 1; value <= updates; value++ {
		var ok bool
		if len(useUpdate) != 0 && useUpdate[0] {
			ok = m.Update("target", func(v *int) { *v = value })
		} else {
			ok = m.Set("target", value)
		}
		if !ok {
			t.Fatal("updating existing key failed")
		}
	}
	current, ok := m.LoadItem("target")
	if !ok || current.Value() != updates {
		t.Fatal("registered value is incorrect before GC")
	}
	return m, original, weak.Make(current)
}

func TestRegisteredEntrySurvivesGC(t *testing.T) {
	for _, useUpdate := range []bool{false, true} {
		method := "Set"
		if useUpdate {
			method = "Update"
		}
		for _, callerOwned := range []bool{false, true} {
			name := "Set"
			if callerOwned {
				name = "StoreItem"
			}
			for _, updates := range []int{0, 1, 2} {
				label := "initial"
				if updates == 1 {
					label = "updated_once"
				} else if updates == 2 {
					label = "updated_twice"
				}
				t.Run(name+"/"+method+"/"+label, func(t *testing.T) {
					m, original, current := prepareRegisteredEntry(t, callerOwned, updates, useUpdate)
					defer func() {
						runtime.KeepAlive(original)
						runtime.KeepAlive(m)
					}()
					for range 10 {
						runtime.GC()
					}
					// Do not traverse the map if its registered entry was collected.
					if current.Value() == nil {
						t.Fatal("current registered entry was collected; map and caller-owned original remain alive")
					}
					if value, ok := m.Get("target"); !ok || value != updates {
						t.Fatalf("Get after GC = (%d, %v), want (%d, true)", value, ok, updates)
					}
				})
			}
		}
	}
}
