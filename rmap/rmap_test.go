package rmap_test

import (
	"fmt"
	smap "github.com/kazu/skiplistmap"
	"math/rand"
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
	"github.com/stretchr/testify/assert"
)

func key(i int) string {
	return fmt.Sprintf("hoge%d", i)
}

// Stored values are returned as stored and survive garbage collection.
func Test_Get_Set(t *testing.T) {
	const cnt = 1000
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < cnt; i++ {
		m.Set(smap.StringKey(key(i)), i)
		if i%97 == 0 {
			runtime.GC()
		}
	}
	runtime.GC()

	for i := 0; i < cnt; i++ {
		v, ok := m.Get(smap.StringKey(key(i)))
		assert.Truef(t, ok, "Get(%q)", key(i))
		assert.Equal(t, i, v)
	}
	assert.Equal(t, cnt, m.Len())
}

// Get, Delete and Len on an empty RMap must not panic.
func Test_Empty(t *testing.T) {
	m := rmap.New[smap.StringKey, int]()

	v, ok := m.Get("missing")
	assert.False(t, ok)
	assert.Zero(t, v)
	assert.False(t, m.Delete("missing"))
	assert.Equal(t, 0, m.Len())
}

// Get of a key that was never stored returns (nil, false) on a filled map.
func Test_Get_Missing(t *testing.T) {
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < 100; i++ {
		m.Set(smap.StringKey(key(i)), i)
	}
	v, ok := m.Get("missing")
	assert.False(t, ok)
	assert.Zero(t, v)
}

// Set of an existing key replaces its value without changing Len.
func Test_Update(t *testing.T) {
	const cnt = 1000
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < cnt; i++ {
		m.Set(smap.StringKey(key(i)), i)
	}
	for i := 0; i < cnt; i++ {
		assert.Truef(t, m.Set(smap.StringKey(key(i)), -i), "Set(%q) update", key(i))
		v, ok := m.Get(smap.StringKey(key(i)))
		assert.Truef(t, ok, "Get(%q) after update", key(i))
		assert.Equal(t, -i, v)
	}
	assert.Equal(t, cnt, m.Len())
}

// Delete removes one key: Get misses at once and after the read map is
// rebuilt, Len shrinks, a second Delete is false, and Set of the same key
// stores the new value again.
func Test_Delete_Reinsert(t *testing.T) {
	const cnt = 1000
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < cnt; i++ {
		m.Set(smap.StringKey(key(i)), i)
	}

	for i := 0; i < cnt; i += 7 {
		assert.Truef(t, m.Delete(smap.StringKey(key(i))), "Delete(%q)", key(i))
		v, ok := m.Get(smap.StringKey(key(i)))
		assert.Falsef(t, ok, "Get(%q) found after Delete", key(i))
		assert.Zero(t, v)
		assert.Falsef(t, m.Delete(smap.StringKey(key(i))), "second Delete(%q)", key(i))
	}
	deleted := (cnt + 6) / 7
	assert.Equal(t, cnt-deleted, m.Len())

	// the deleted keys must stay absent on repeated reads
	for round := 0; round < 3; round++ {
		for i := 0; i < cnt; i += 7 {
			_, ok := m.Get(smap.StringKey(key(i)))
			assert.Falsef(t, ok, "Get(%q) found on repeated read", key(i))
		}
	}
	assert.Equal(t, cnt-deleted, m.Len())

	for i := 0; i < cnt; i += 7 {
		assert.Truef(t, m.Set(smap.StringKey(key(i)), -i), "Set(%q) after Delete", key(i))
		v, ok := m.Get(smap.StringKey(key(i)))
		assert.Truef(t, ok, "Get(%q) after re-insert", key(i))
		assert.Equal(t, -i, v)
	}
	assert.Equal(t, cnt, m.Len())

	for i := 0; i < cnt; i++ {
		v, ok := m.Get(smap.StringKey(key(i)))
		assert.Truef(t, ok, "Get(%q)", key(i))
		if i%7 == 0 {
			assert.Equal(t, -i, v)
		} else {
			assert.Equal(t, i, v)
		}
	}
}

// After every key is promoted to the read map, Delete of a read key, of a
// missing key and re-insert behave the same as on the dirty map.
func Test_Delete_AfterPromotion(t *testing.T) {
	const cnt = 100
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < cnt; i++ {
		m.Set(smap.StringKey(key(i)), i)
	}
	// enough misses to rebuild the read map from the dirty map
	for i := 0; i < 2*cnt; i++ {
		m.Get("missing")
	}

	assert.True(t, m.Delete(smap.StringKey(key(5))))
	_, ok := m.Get(smap.StringKey(key(5)))
	assert.False(t, ok)
	assert.False(t, m.Delete(smap.StringKey(key(5))))
	assert.False(t, m.Delete("missing"))
	assert.Equal(t, cnt-1, m.Len())

	assert.True(t, m.Set(smap.StringKey(key(5)), -5))
	v, ok := m.Get(smap.StringKey(key(5)))
	assert.True(t, ok)
	assert.Equal(t, -5, v)
	assert.Equal(t, cnt, m.Len())

	assert.True(t, m.Delete(smap.StringKey(key(5))))
	_, ok = m.Get(smap.StringKey(key(5)))
	assert.False(t, ok)
	assert.Equal(t, cnt-1, m.Len())
}

// After every key of the read map is deleted, a new key is still found by
// Get, counted by Len and removed by Delete.
func Test_Set_AfterAllReadKeysDeleted(t *testing.T) {
	for _, cnt := range []int{1, 100} {
		t.Run(fmt.Sprintf("n=%d", cnt), func(t *testing.T) {
			m := rmap.New[smap.StringKey, int]()
			for i := 0; i < cnt; i++ {
				m.Set(smap.StringKey(key(i)), i)
			}
			// enough misses to rebuild the read map from the dirty map
			for i := 0; i < 2*cnt; i++ {
				m.Get("missing")
			}
			for i := 0; i < cnt; i++ {
				assert.Truef(t, m.Delete(smap.StringKey(key(i))), "Delete(%q)", key(i))
			}
			assert.Equal(t, 0, m.Len())

			assert.True(t, m.Set("new", -1))
			v, ok := m.Get("new")
			assert.True(t, ok)
			assert.Equal(t, -1, v)
			assert.Equal(t, 1, m.Len())
			assert.True(t, m.Delete("new"))
			_, ok = m.Get("new")
			assert.False(t, ok)
			assert.Equal(t, 0, m.Len())
		})
	}
}

// Once every key is deleted, a miss finds an empty read map and an empty
// dirty map: repeated Get and Delete of an absent key must not rebuild them
// (AllocsPerRun leaves its first call out).
func Test_Miss_AfterAllKeysDeleted(t *testing.T) {
	m := rmap.New[smap.StringKey, int]()
	m.Set("a", 1)
	m.Delete("a")
	assert.Equal(t, 0, m.Len())

	allocs := testing.AllocsPerRun(100, func() {
		m.Get("missing")
		m.Delete("missing")
	})
	assert.Equal(t, 0.0, allocs, "allocations per Get and Delete of an absent key")
	_, ok := m.Get("a")
	assert.False(t, ok)
}

// A key that lives only in the dirty map is deleted and set again while the
// dirty map keeps enough live keys that no read on it promotes (every dirty
// read counts as a miss, promotion happens once misses reach dirty.Len()),
// so the re-Set goes into the dirty map that still holds the marked item
// (reusing its slot or appending a new one, depending on the hash order).
// The value and Len must be right before and after the next promotion.
func Test_Delete_Reinsert_InDirty(t *testing.T) {
	const cnt = 100
	m := rmap.New[smap.StringKey, int]()
	for i := 0; i < cnt; i++ {
		m.Set(smap.StringKey(key(i)), i)
	}
	for i := 0; i < 2*cnt; i++ {
		m.Get("missing")
	}
	// promoted: the next new keys go to the dirty map
	assert.True(t, m.Set("dirty", 1))
	assert.True(t, m.Set("d2", 2))
	assert.True(t, m.Set("d3", 3))
	assert.True(t, m.Set("d4", 4))
	assert.Equal(t, cnt+4, m.Len())
	assert.True(t, m.Delete("dirty")) // misses 1, dirty.Len 3
	_, ok := m.Get("dirty")           // misses 2
	assert.False(t, ok)
	assert.Equal(t, cnt+3, m.Len())
	assert.True(t, m.Set("dirty", 5)) // into the same dirty map, dirty.Len 4
	v, ok := m.Get("dirty")           // misses 3
	assert.True(t, ok)
	assert.Equal(t, 5, v)
	assert.Equal(t, cnt+4, m.Len())

	for i := 0; i < 2*cnt; i++ {
		m.Get("missing")
	}
	for k, want := range map[string]int{"dirty": 5, "d2": 2, "d3": 3, "d4": 4} {
		v, ok := m.Get(smap.StringKey(k))
		assert.Truef(t, ok, "Get(%q) after promotion", k)
		assert.Equal(t, want, v)
	}
	assert.Equal(t, cnt+4, m.Len())
}

// Random Set, Get and Delete steps checked against a Go map after every step.
func Test_SequentialModel(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		r := rand.New(rand.NewSource(seed))
		nkeys := 1 + r.Intn(40)
		m := rmap.New[smap.StringKey, int]()
		want := map[string]int{}
		for step := 0; step < 2000; step++ {
			k := key(r.Intn(nkeys))
			switch op := r.Intn(10); {
			case op < 4:
				m.Set(smap.StringKey(k), step)
				want[k] = step
			case op < 7:
				v, ok := m.Get(smap.StringKey(k))
				wv, live := want[k]
				if ok != live || (live && v != wv) {
					t.Fatalf("seed %d step %d: Get(%q) = (%v, %v), want (%v, %v)", seed, step, k, v, ok, wv, live)
				}
			case op < 9:
				_, live := want[k]
				if got := m.Delete(smap.StringKey(k)); got != live {
					t.Fatalf("seed %d step %d: Delete(%q) = %v, want %v", seed, step, k, got, live)
				}
				delete(want, k)
			default:
				// a miss counts towards rebuilding the read map
				if _, ok := m.Get("missing"); ok {
					t.Fatalf("seed %d step %d: Get(missing) found", seed, step)
				}
			}
			if got := m.Len(); got != len(want) {
				t.Fatalf("seed %d step %d: Len() = %d, want %d", seed, step, got, len(want))
			}
		}
	}
}
