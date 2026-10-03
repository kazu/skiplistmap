package skiplistmap

import (
	"fmt"
	"runtime"
	"testing"
)

// Check allocation policy separately from the lifetime of the published copy.
func TestExternalReplacementDoesNotAllocatePoolSlot(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, method := range []string{"Set", "Update"} {
			t.Run(fmt.Sprintf("embedded=%v/%s", embedded, method), func(t *testing.T) {
				m := New[IntKey, int](UseEmbeddedPool[IntKey, int](embedded))
				original := NewEntry[IntKey, int](1, 0)
				defer runtime.KeepAlive(original)
				if !m.StoreItem(original) {
					t.Fatal("StoreItem")
				}
				pool := m.pooler
				before := allocatedPoolSlots(m)
				var ok bool
				if method == "Set" {
					ok = m.Set(1, 1)
				} else {
					ok = m.Update(1, func(v *int) { *v = 1 })
				}
				if !ok {
					t.Fatal("update")
				}
				if m.pooler != pool || allocatedPoolSlots(m) != before {
					t.Fatal("updating a caller-owned entry allocated a pool slot")
				}
			})
		}
	}
}

func allocatedPoolSlots(m *Map[IntKey, int]) int {
	if m.pooler == nil {
		return 0
	}
	count := 0
	for i := range m.pooler.itemPool {
		head := m.pooler.itemPool[i].Next()
		if !head.Empty() {
			count += samepleItemPoolFromListHead[IntKey, int](head).items.Len()
		}
	}
	return count
}
