package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"testing"

	"github.com/kazu/skiplistmap"
)

func TestEntryValues(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
			keys := []skiplistmap.IntKey{1, 2, 3}
			sort.Slice(keys, func(i, j int) bool {
				a, _ := keys[i].KeyHash()
				b, _ := keys[j].KeyHash()
				return bits.Reverse64(a) < bits.Reverse64(b)
			})
			for _, read := range []func() (int, bool){m.First, m.Last, func() (int, bool) { return m.SearchKey(0) }} {
				if value, found := read(); found || value != 0 {
					t.Fatalf("empty map returned (%d,%v)", value, found)
				}
			}
			for i, key := range keys {
				m.Set(key, i)
				hash, _ := key.KeyHash()
				if value, found := m.SearchKey(hash); !found || value != i {
					t.Fatalf("SearchKey=(%d,%v), want (%d,true)", value, found, i)
				}
			}
			if value, found := m.First(); !found || value != 0 {
				t.Fatalf("First=(%d,%v), want (0,true)", value, found)
			}
			if value, found := m.Last(); !found || value != 2 {
				t.Fatalf("Last=(%d,%v), want (2,true)", value, found)
			}
			m.Delete(keys[0])
			m.Purge(keys[2])
			for _, read := range []func() (int, bool){m.First, m.Last} {
				if value, found := read(); !found || value != 1 {
					t.Fatalf("remaining end=(%d,%v), want (1,true)", value, found)
				}
			}
		})
	}
}

func TestEmbeddedEntryAccess(t *testing.T) {
	for _, populated := range []bool{false, true} {
		m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](true))
		if populated {
			m.Set(1, 10)
		}
		hash, conflict := skiplistmap.IntKey(1).KeyHash()
		for name, call := range map[string]func(){
			"LoadItem":       func() { m.LoadItem(1) },
			"LoadItemByHash": func() { m.LoadItemByHash(hash, conflict) },
			"RangeItem": func() {
				m.RangeItem(func(*skiplistmap.Entry[skiplistmap.IntKey, int]) bool {
					t.Error("embedded entry escaped to callback")
					return false
				})
			},
		} {
			t.Run(fmt.Sprintf("%s/populated=%v", name, populated), func(t *testing.T) {
				defer func() {
					p := recover()
					if p == nil || !strings.Contains(fmt.Sprint(p), "entry access is unavailable with UseEmbeddedPool") {
						t.Fatalf("panic=%v, want explicit embedded-entry restriction", p)
					}
				}()
				call()
			})
		}
		if got, ok := m.Get(1); ok != populated || populated && got != 10 {
			t.Fatalf("Get=(%d,%v), populated=%v", got, ok, populated)
		}
		if got, ok := m.GetByHash(hash, conflict); ok != populated || populated && got != 10 {
			t.Fatalf("GetByHash=(%d,%v), populated=%v", got, ok, populated)
		}
		count := 0
		m.Range(func(k skiplistmap.IntKey, v int) bool {
			if k != 1 || v != 10 {
				t.Fatalf("Range=(%d,%d)", k, v)
			}
			count++
			return true
		})
		if count != m.Len() {
			t.Fatalf("Range count=%d, Len=%d", count, m.Len())
		}
		for key := range m.Keys() {
			if key != 1 {
				t.Fatalf("Keys returned %d", key)
			}
		}
	}
}

func TestNonEmbeddedEntryAccess(t *testing.T) {
	m := skiplistmap.New[skiplistmap.IntKey, int]()
	m.Set(1, 10)
	e, ok := m.LoadItem(1)
	if !ok || e.Value() != 10 {
		t.Fatal("LoadItem failed")
	}
	hash, conflict := skiplistmap.IntKey(1).KeyHash()
	if byHash, ok := m.LoadItemByHash(hash, conflict); !ok || byHash != e {
		t.Fatal("LoadItemByHash failed")
	}
	for name, read := range map[string]func() (int, bool){
		"First": m.First, "Last": m.Last,
		"SearchKey": func() (int, bool) { return m.SearchKey(hash) },
	} {
		if value, ok := read(); !ok || value != 10 {
			t.Fatalf("%s=(%d,%v), want (10,true)", name, value, ok)
		}
	}
	count := 0
	m.RangeItem(func(got *skiplistmap.Entry[skiplistmap.IntKey, int]) bool {
		if got != e {
			t.Fatal("RangeItem returned a different entry")
		}
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("RangeItem count=%d", count)
	}
}
