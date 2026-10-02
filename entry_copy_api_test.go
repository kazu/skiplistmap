package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	smap "github.com/kazu/skiplistmap"
)

func TestEntryCopyShallowIndependentRoot(t *testing.T) {
	value := new(int)
	*value = 10
	e := smap.NewEntry[smap.IntKey, *int](1, value)
	m := smap.New[smap.IntKey, *int]()
	if !m.StoreItem(e) || !m.Purge(1) {
		t.Fatal("fixture failed")
	}
	fresh := e.Copy()
	if fresh == e || fresh.Key() != e.Key() || fresh.Value() != value || fresh.IsDeleted() || !fresh.PtrListHead().IsSingle() {
		t.Fatal("Copy did not create a fresh entry with the same shallow payload")
	}
	if !m.StoreItem(fresh) || !m.Set(1, nil) {
		t.Fatal("copy could not be inserted and updated")
	}
	if e.Value() != value || fresh.Value() != value {
		t.Fatal("replacement modified an earlier entry")
	}
	if m.StoreItem(e) {
		t.Fatal("Copy erased source retirement")
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(fresh)
}

func TestEntryCopyStoreItemOrCopy(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := smap.New[smap.IntKey, int](smap.UseEmbeddedPool[smap.IntKey, int](embedded))
			e := smap.NewEntry[smap.IntKey, int](1, 10)
			got, ok := m.StoreItemOrCopy(e)
			if !ok || got != e {
				t.Fatal("fresh entry was copied or refused")
			}
			if got, ok := m.StoreItemOrCopy(e); ok || got != nil {
				t.Fatal("linked entry was copied")
			}
			if !m.Purge(1) {
				t.Fatal("Purge failed")
			}
			fresh, ok := m.StoreItemOrCopy(e)
			if !ok || fresh == e || fresh == nil {
				t.Fatal("retired entry was not copied")
			}
			if stored, ok := m.LoadItem(1); !ok || stored != fresh {
				t.Fatal("copy was not linked")
			}
			if !m.Set(2, 20) {
				t.Fatal("Set failed")
			}
			pooled, _ := m.LoadItem(2)
			if got, ok := m.StoreItemOrCopy(pooled); ok || got != nil {
				t.Fatal("pool entry was copied")
			}
			if !m.Purge(2) {
				t.Fatal("pool Purge failed")
			}
			if got, ok := m.StoreItemOrCopy(pooled); ok || got != nil {
				t.Fatal("retired pool entry was copied")
			}
			runtime.KeepAlive(e)
			runtime.KeepAlive(fresh)
		})
	}
}

func TestEntryCopyStoreItemOrCopyExistingKey(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			a := smap.New[smap.IntKey, int](smap.UseEmbeddedPool[smap.IntKey, int](embedded))
			b := smap.New[smap.IntKey, int](smap.UseEmbeddedPool[smap.IntKey, int](embedded))
			e := smap.NewEntry[smap.IntKey, int](1, 10)
			if !a.StoreItem(e) || !a.Delete(1) || !b.Set(1, 20) {
				t.Fatal("fixture failed")
			}
			fresh, ok := b.StoreItemOrCopy(e)
			if !ok || fresh == nil || fresh == e {
				t.Fatal("retired entry was not copied for update")
			}
			if got, ok := b.Get(1); !ok || got != 10 || b.Len() != 1 || a.Len() != 0 {
				t.Fatal("existing-key update changed StoreItem semantics")
			}
			if stored, _ := b.LoadItem(1); stored == fresh || !fresh.PtrListHead().IsSingle() {
				t.Fatal("update linked the supplied copy")
			}
			runtime.KeepAlive(e)
			runtime.KeepAlive(fresh)
		})
	}
}

func ExampleEntry_Copy() {
	m := smap.New[smap.IntKey, *int]()
	value := new(int)
	e := smap.NewEntry[smap.IntKey, *int](1, value)
	m.StoreItem(e)
	m.Purge(1)
	fresh := e.Copy()
	fmt.Println(fresh != e, fresh.Value() == value, m.StoreItem(fresh))
	runtime.KeepAlive(e)
	runtime.KeepAlive(fresh)
	// Output: true true true
}

func ExampleMap_StoreItemOrCopy() {
	m := smap.New[smap.IntKey, string]()
	e := smap.NewEntry[smap.IntKey, string](1, "value")
	m.StoreItem(e)
	m.Purge(1)
	fresh, ok := m.StoreItemOrCopy(e)
	fmt.Println(ok, fresh != e)
	fmt.Println(m.Get(1))
	runtime.KeepAlive(e)
	runtime.KeepAlive(fresh)
	// Output:
	// true true
	// value true
}
