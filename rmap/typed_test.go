package rmap_test

import (
	"reflect"
	"testing"

	smap "github.com/kazu/skiplistmap"
	"github.com/kazu/skiplistmap/rmap"
)

type collidingKey struct {
	id    int
	label []byte
}

func (k collidingKey) KeyHash() (uint64, uint64)     { return 7, 11 }
func (k collidingKey) Equal(other collidingKey) bool { return k.id == other.id }

func TestTypedKeyEquality(t *testing.T) {
	m := rmap.New[collidingKey, int]()
	first := collidingKey{1, []byte("first")}
	second := collidingKey{2, []byte("second")}
	for _, key := range []collidingKey{first, second} {
		if !m.Set(key, key.id) {
			t.Fatal("Set failed")
		}
	}
	for round := 0; round < 4; round++ {
		for _, key := range []collidingKey{first, second} {
			alias := collidingKey{key.id, []byte("equal key, distinct representation")}
			if got, ok := m.Get(alias); !ok || got != key.id {
				t.Fatalf("Get(%d) = (%d, %v)", key.id, got, ok)
			}
		}
	}
	if !m.Set(collidingKey{1, nil}, 9) || m.Len() != 2 {
		t.Fatal("Equal key was not updated")
	}
	if got, ok := m.Get(second); !ok || got != 2 {
		t.Fatalf("collision changed: (%d, %v)", got, ok)
	}
	if !m.Delete(collidingKey{1, nil}) || m.Len() != 1 {
		t.Fatal("Equal key was not deleted")
	}
	if got, ok := m.Get(first); ok || got != 0 {
		t.Fatalf("deleted key = (%d, %v)", got, ok)
	}
	if !m.Set(first, 3) || m.Len() != 2 {
		t.Fatal("reinsertion failed")
	}
	if got, ok := m.Get(first); !ok || got != 3 {
		t.Fatalf("reinserted key = (%d, %v)", got, ok)
	}
}

func checkTypedValue[V any](t *testing.T, first, second V) {
	t.Helper()
	m := rmap.New[smap.IntKey, V]()
	for _, value := range []V{first, second} {
		if !m.Set(1, value) {
			t.Fatal("Set failed")
		}
		for i := 0; i < 3; i++ {
			got, ok := m.Get(1)
			if !ok || !reflect.DeepEqual(got, value) {
				t.Fatalf("Get = (%v, %v), want %v", got, ok, value)
			}
		}
	}
	hash, conflict := smap.IntKey(1).KeyHash()
	if got, ok := m.Get2(hash, conflict); !ok || !reflect.DeepEqual(got, second) {
		t.Fatalf("Get2 = (%v, %v)", got, ok)
	}
	if !m.Delete(1) || !m.Set2(hash, conflict, 1, first) {
		t.Fatal("Delete/Set2 failed")
	}
	if got, ok := m.Get(1); !ok || !reflect.DeepEqual(got, first) || m.Len() != 1 {
		t.Fatalf("reinsertion = (%v, %v), len %d", got, ok, m.Len())
	}
	var zero V
	if got, ok := m.Get(2); ok || !reflect.DeepEqual(got, zero) {
		t.Fatalf("absent = (%v, %v)", got, ok)
	}
}

func TestTypedValues(t *testing.T) {
	t.Run("int", func(t *testing.T) { checkTypedValue(t, 1, 2) })
	t.Run("struct", func(t *testing.T) { checkTypedValue(t, [16]int{1}, [16]int{2}) })
	t.Run("pointer", func(t *testing.T) { checkTypedValue(t, new(int), (*int)(nil)) })
	t.Run("slice", func(t *testing.T) { checkTypedValue(t, []int{1}, []int(nil)) })
	t.Run("any", func(t *testing.T) { checkTypedValue[any](t, 1, nil) })
}

func TestBytesKey(t *testing.T) {
	m := rmap.New[smap.BytesKey, string]()
	m.Set(smap.BytesKey("key"), "value")
	for i := 0; i < 3; i++ {
		if got, ok := m.Get(smap.BytesKey("key")); !ok || got != "value" {
			t.Fatalf("Get = (%q, %v)", got, ok)
		}
	}
}
