package rmap_test

import (
	"fmt"
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
	m := rmap.New()
	for i := 0; i < cnt; i++ {
		m.Set(key(i), i)
		if i%97 == 0 {
			runtime.GC()
		}
	}
	runtime.GC()

	for i := 0; i < cnt; i++ {
		v, ok := m.Get(key(i))
		assert.Truef(t, ok, "Get(%q)", key(i))
		assert.Equal(t, i, v)
	}
	assert.Equal(t, cnt, m.Len())
}

// Get, Delete and Len on an empty RMap must not panic.
func Test_Empty(t *testing.T) {
	m := rmap.New()

	v, ok := m.Get("missing")
	assert.False(t, ok)
	assert.Nil(t, v)
	assert.False(t, m.Delete("missing"))
	assert.Equal(t, 0, m.Len())
}

// Get of a key that was never stored returns (nil, false) on a filled map.
func Test_Get_Missing(t *testing.T) {
	m := rmap.New()
	for i := 0; i < 100; i++ {
		m.Set(key(i), i)
	}
	v, ok := m.Get("missing")
	assert.False(t, ok)
	assert.Nil(t, v)
}

// Set of an existing key replaces its value without changing Len.
func Test_Update(t *testing.T) {
	const cnt = 1000
	m := rmap.New()
	for i := 0; i < cnt; i++ {
		m.Set(key(i), i)
	}
	for i := 0; i < cnt; i++ {
		assert.Truef(t, m.Set(key(i), -i), "Set(%q) update", key(i))
		v, ok := m.Get(key(i))
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
	m := rmap.New()
	for i := 0; i < cnt; i++ {
		m.Set(key(i), i)
	}

	for i := 0; i < cnt; i += 7 {
		assert.Truef(t, m.Delete(key(i)), "Delete(%q)", key(i))
		v, ok := m.Get(key(i))
		assert.Falsef(t, ok, "Get(%q) found after Delete", key(i))
		assert.Nil(t, v)
		assert.Falsef(t, m.Delete(key(i)), "second Delete(%q)", key(i))
	}
	deleted := (cnt + 6) / 7
	assert.Equal(t, cnt-deleted, m.Len())

	// the deleted keys must stay absent on repeated reads
	for round := 0; round < 3; round++ {
		for i := 0; i < cnt; i += 7 {
			_, ok := m.Get(key(i))
			assert.Falsef(t, ok, "Get(%q) found on repeated read", key(i))
		}
	}
	assert.Equal(t, cnt-deleted, m.Len())

	for i := 0; i < cnt; i += 7 {
		assert.Truef(t, m.Set(key(i), -i), "Set(%q) after Delete", key(i))
		v, ok := m.Get(key(i))
		assert.Truef(t, ok, "Get(%q) after re-insert", key(i))
		assert.Equal(t, -i, v)
	}
	assert.Equal(t, cnt, m.Len())

	for i := 0; i < cnt; i++ {
		v, ok := m.Get(key(i))
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
	m := rmap.New()
	for i := 0; i < cnt; i++ {
		m.Set(key(i), i)
	}
	// enough misses to rebuild the read map from the dirty map
	for i := 0; i < 2*cnt; i++ {
		m.Get("missing")
	}

	assert.True(t, m.Delete(key(5)))
	_, ok := m.Get(key(5))
	assert.False(t, ok)
	assert.False(t, m.Delete(key(5)))
	assert.False(t, m.Delete("missing"))
	assert.Equal(t, cnt-1, m.Len())

	assert.True(t, m.Set(key(5), -5))
	v, ok := m.Get(key(5))
	assert.True(t, ok)
	assert.Equal(t, -5, v)
	assert.Equal(t, cnt, m.Len())

	assert.True(t, m.Delete(key(5)))
	_, ok = m.Get(key(5))
	assert.False(t, ok)
	assert.Equal(t, cnt-1, m.Len())
}

// A key that lives only in the dirty map is deleted and set again while the
// dirty map keeps enough live keys that no read on it promotes (every dirty
// read counts as a miss, promotion happens once misses reach dirty.Len()),
// so the re-Set goes into the dirty map that still holds the marked item
// (reusing its slot or appending a new one, depending on the hash order).
// The value and Len must be right before and after the next promotion.
func Test_Delete_Reinsert_InDirty(t *testing.T) {
	const cnt = 100
	m := rmap.New()
	for i := 0; i < cnt; i++ {
		m.Set(key(i), i)
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
		v, ok := m.Get(k)
		assert.Truef(t, ok, "Get(%q) after promotion", k)
		assert.Equal(t, want, v)
	}
	assert.Equal(t, cnt+4, m.Len())
}
