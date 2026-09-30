package rmap

import (
	"testing"

	smap "github.com/kazu/skiplistmap"
)

func TestReadSlotIdentity(t *testing.T) {
	for _, differentConflict := range []bool{true, false} {
		m := New()
		m.Set("original", 1)
		m.Get("original")
		k, conflict := smap.KeyToHash("original")
		if differentConflict {
			conflict ^= 1
		}
		read := m.read.Load()
		if read.store2(m, k, conflict, "collision", 2) {
			t.Errorf("different conflict %v: replaced another key", differentConflict)
		}
		if got, ok := m.Get("original"); !ok || got != 1 {
			t.Errorf("different conflict %v: original = (%v, %v)", differentConflict, got, ok)
		}
	}
}

func TestReadCollisionsSurvivePromotion(t *testing.T) {
	for _, sameConflict := range []bool{false, true} {
		m := New()
		g := m.read.Load().generation
		read := &readMap{generation: g, m: make(map[uint64]*readSlot)}
		for i, key := range []string{"first", "second"} {
			conflict := uint64(i)
			if sameConflict {
				conflict = 0
			}
			slot := &readSlot{key: key, conflict: conflict}
			slot.value.Store(newStoredValue(i))
			read.addSlot(7, slot)
		}
		m.read.Store(read)
		count := int64(2)
		m.len.Store(count)
		for round := 0; round < 2; round++ {
			m.promote(m.read.Load().generation)
			for i, key := range []string{"first", "second"} {
				conflict := uint64(i)
				if sameConflict {
					conflict = 0
				}
				if got, ok := m.get(7, conflict, key, true); !ok || got != i {
					t.Fatalf("same conflict %v: %s = (%v, %v)", sameConflict, key, got, ok)
				}
			}
			if m.Len() != 2 {
				t.Fatalf("Len = %d, want 2", m.Len())
			}
		}
		conflict := uint64(1)
		if sameConflict {
			conflict = 0
		}
		if !m.read.Load().store2(m, 7, conflict, "second", 9) {
			t.Fatal("could not update the second colliding key")
		}
		if got, ok := m.get(7, 0, "first", true); !ok || got != 0 {
			t.Fatalf("updating second changed first: (%v, %v)", got, ok)
		}
		if ok, found := m.deleteRead(m.read.Load(), 7, 0, "first"); !ok || !found {
			t.Fatal("could not delete the first colliding key")
		}
		m.promote(m.read.Load().generation)
		if got, ok := m.get(7, conflict, "second", true); !ok || got != 9 || m.Len() != 1 {
			t.Fatalf("remaining collision = (%v, %v), Len = %d", got, ok, m.Len())
		}
	}
}

func TestReadUpdateCallback(t *testing.T) {
	m := New()
	var values []interface{}
	m.onNewStores = []func(smap.MapItem){func(item smap.MapItem) {
		if item.Key() != "key" {
			t.Errorf("callback key = %v", item.Key())
		}
		values = append(values, item.Value())
	}}
	m.Set("key", 1)
	m.Set("key", 2)
	if len(values) != 0 {
		t.Fatal("dirty updates unexpectedly invoked callbacks")
	}
	m.Get("key") // Promote to read.
	for i, value := range []interface{}{3, "changed type", nil} {
		m.Set("key", value)
		if len(values) != i+1 || values[i] != value {
			t.Fatalf("callbacks = %v after setting %v", values, value)
		}
		if got, ok := m.Get("key"); !ok || got != value {
			t.Fatalf("Get = (%v, %v), want (%v, true)", got, ok, value)
		}
	}
	m.Delete("key")
	m.Set("key", 4)
	if len(values) != 4 || values[3] != 4 || m.Len() != 1 {
		t.Fatalf("revival: callbacks = %v, Len = %d", values, m.Len())
	}
}
