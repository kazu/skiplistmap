package skiplistmap_test

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	smap "github.com/kazu/skiplistmap"
)

type typedSliceKey struct{ data []byte }

func (k typedSliceKey) KeyHash() (uint64, uint64)      { return 1, 1 }
func (k typedSliceKey) Equal(other typedSliceKey) bool { return bytes.Equal(k.data, other.data) }

type typedPointerKey struct{ id int }

func (k *typedPointerKey) KeyHash() (uint64, uint64) { return 0, 0 }
func (k *typedPointerKey) Equal(other *typedPointerKey) bool {
	if k == nil || other == nil {
		return k == other
	}
	return k.id == other.id
}

func TestTypedDumpIncludesDummies(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		m := smap.New[smap.StringKey, int](smap.UseEmbeddedPool[smap.StringKey, int](embedded))
		m.Set("entry", 7)
		var output bytes.Buffer
		m.DumpEntry(&output)
		m.DumpBucket(&output)
		m.DumpBucketPerLevel(&output)
		if output.Len() == 0 {
			t.Fatal("empty diagnostic output")
		}
	}
}

type typedPayload struct {
	Tag   byte
	Words [17]uint64
	Text  string
	Data  []byte
	Ref   *int
}

func typedValue(n int) typedPayload {
	v := typedPayload{Tag: byte(n), Text: fmt.Sprint(n), Data: []byte{byte(n)}, Ref: new(int)}
	*v.Ref = n
	for i := range v.Words {
		v.Words[i] = uint64(n)
	}
	return v
}

func testTypedOperations[K smap.Key[K], V any](t *testing.T, keys []K, first, second V) {
	t.Helper()
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprintf("embedded=%v", embedded), func(t *testing.T) {
			m := smap.New[K, V](smap.UseEmbeddedPool[K, V](embedded), smap.MaxPefBucket[K, V](4))
			for _, key := range keys {
				if !m.Set(key, first) {
					t.Fatal("initial Set")
				}
				if value, ok := m.Get(key); !ok || !reflect.DeepEqual(value, first) {
					t.Fatalf("Get=(%v,%v)", value, ok)
				}
			}
			for _, key := range keys {
				if !m.Set(key, second) {
					t.Fatal("replacement Set")
				}
				if value, ok := m.Get(key); !ok || !reflect.DeepEqual(value, second) {
					t.Fatalf("updated Get=(%v,%v)", value, ok)
				}
			}
			seen := 0
			for key, value := range m.All() {
				found := false
				for _, wanted := range keys {
					found = found || key.Equal(wanted)
				}
				if !found || !reflect.DeepEqual(value, second) {
					t.Fatalf("All=(%v,%v)", key, value)
				}
				seen++
			}
			if seen != len(keys) || m.Len() != len(keys) {
				t.Fatalf("count=%d len=%d", seen, m.Len())
			}
			count := 0
			for range m.Keys() {
				count++
				break
			}
			if count != 1 {
				t.Fatal("Keys early exit")
			}
			count = 0
			for value := range m.Values() {
				if !reflect.DeepEqual(value, second) {
					t.Fatal("Values")
				}
				count++
				break
			}
			if count != 1 {
				t.Fatal("Values early exit")
			}
			for i, key := range keys {
				var ok bool
				if i%2 == 0 {
					ok = m.Delete(key)
				} else {
					ok = m.Purge(key)
				}
				if !ok {
					t.Fatal("delete")
				}
				if _, ok := m.Get(key); ok {
					t.Fatal("deleted key found")
				}
			}
			if m.Len() != 0 {
				t.Fatalf("remaining len=%d", m.Len())
			}
		})
	}
}

func TestTypedStandardKeys(t *testing.T) {
	t.Run("string", func(t *testing.T) { testTypedOperations(t, []smap.StringKey{"", "a", "b"}, 0, 1) })
	t.Run("bytes", func(t *testing.T) {
		testTypedOperations(t, []smap.BytesKey{nil, []byte("a"), []byte("b")}, []byte(nil), []byte{1, 2})
	})
	t.Run("uint64", func(t *testing.T) {
		testTypedOperations(t, []smap.Uint64Key{0, 1, 1 << 63, ^smap.Uint64Key(0)}, typedValue(0), typedValue(1))
	})
	t.Run("byte", func(t *testing.T) { testTypedOperations(t, []smap.ByteKey{0, 1, 255}, uint64(0), uint64(2)) })
	t.Run("int", func(t *testing.T) { testTypedOperations(t, []smap.IntKey{0, -1, 1}, struct{}{}, struct{}{}) })
	t.Run("int32", func(t *testing.T) { testTypedOperations(t, []smap.Int32Key{0, -1, 1}, (*int)(nil), new(int)) })
	t.Run("uint32", func(t *testing.T) {
		testTypedOperations(t, []smap.Uint32Key{0, 1, ^smap.Uint32Key(0)}, [3]byte{}, [3]byte{1, 2, 3})
	})
	t.Run("int64", func(t *testing.T) {
		testTypedOperations[smap.Int64Key, any](t, []smap.Int64Key{0, -1, 1}, nil, []int{1, 2})
	})
}

func TestTypedCustomKeys(t *testing.T) {
	t.Run("slice-struct", func(t *testing.T) {
		testTypedOperations(t, []typedSliceKey{{nil}, {[]byte("a")}, {[]byte("b")}}, typedValue(0), typedValue(1))
	})
	t.Run("pointer", func(t *testing.T) { testTypedOperations(t, []*typedPointerKey{nil, {1}, {2}}, 0, 1) })
	m := smap.New[smap.BytesKey, int]()
	m.Set(nil, 1)
	m.Set(smap.BytesKey{}, 2)
	if value, ok := m.Get(nil); !ok || value != 2 || m.Len() != 1 {
		t.Fatal("nil and empty BytesKey must identify the same key")
	}
}

func TestTypedExternalEntryLayoutAndGC(t *testing.T) {
	type owner struct {
		prefix [7]byte
		entry  smap.Entry[smap.StringKey, typedPayload]
		suffix byte
	}
	root := &owner{suffix: 9}
	if !root.entry.InitEntry("key", typedValue(1)) || root.entry.InitEntry("other", typedValue(2)) {
		t.Fatal("InitEntry must initialize only once")
	}
	m := smap.New[smap.StringKey, typedPayload]()
	if !m.StoreItem(&root.entry) {
		t.Fatal("StoreItem")
	}
	if got, ok := m.LoadItem("key"); !ok || got != &root.entry {
		t.Fatal("external identity")
	}
	for i := 2; i < 32; i++ {
		if !m.Set("key", typedValue(i)) {
			t.Fatal("Set")
		}
		runtime.GC()
	}
	if root.entry.Value().Words[0] != 1 || root.entry.Key() != "key" || root.suffix != 9 {
		t.Fatal("replaced external storage changed")
	}
	if v, ok := m.Get("key"); !ok || v.Words[0] != 31 {
		t.Fatal("replacement lost after GC")
	}
	if root.entry.InitEntry("other", typedValue(2)) {
		t.Fatal("reinitialized linked/retired root")
	}
	runtime.KeepAlive(root)
}

func TestTypedIterators(t *testing.T) {
	m := smap.New[smap.IntKey, int]()
	for _, k := range []smap.IntKey{3, 1, 2} {
		m.Set(k, int(k)*10)
	}
	if got := maps.Collect(m.All()); !reflect.DeepEqual(got, map[smap.IntKey]int{1: 10, 2: 20, 3: 30}) {
		t.Fatal(got)
	}
	if got := slices.Sorted(m.Keys()); !reflect.DeepEqual(got, []smap.IntKey{1, 2, 3}) {
		t.Fatal(got)
	}
	seen := 0
	for range m.All() {
		seen++
		break
	}
	if seen != 1 {
		t.Fatal("All early exit")
	}
}

func TestTypedEmbeddedReplacementRange(t *testing.T) {
	m := smap.New[smap.StringKey, typedPayload](smap.UseEmbeddedPool[smap.StringKey, typedPayload](true), smap.MaxPefBucket[smap.StringKey, typedPayload](8))
	const count = 200
	for i := 0; i < count; i++ {
		m.Set(smap.StringKey(fmt.Sprint(i)), typedValue(i))
	}
	for round := 0; round < 3; round++ {
		for i := 0; i < count; i++ {
			if !m.Set(smap.StringKey(fmt.Sprint(i)), typedValue(i+round)) {
				t.Fatal("Set")
			}
			seen := map[smap.StringKey]bool{}
			for key := range m.Keys() {
				if seen[key] {
					t.Fatalf("duplicate %s after round %d key %d", key, round, i)
				}
				seen[key] = true
			}
			if len(seen) != count || m.Len() != count {
				t.Fatalf("count=%d len=%d", len(seen), m.Len())
			}
		}
	}
}

func TestTypedRangeKeepsPoolsAlive(t *testing.T) {
	const count = 20000
	m := smap.New[smap.StringKey, int](smap.UsePool[smap.StringKey, int](true))
	for i := 0; i < count; i++ {
		m.Set(smap.StringKey(fmt.Sprint(i)), i)
	}
	seen := make(map[smap.StringKey]bool, count)
	m.Range(func(key smap.StringKey, value int) bool {
		if len(seen)%1024 == 0 {
			runtime.GC()
		}
		if seen[key] || string(key) != fmt.Sprint(value) {
			t.Errorf("invalid entry (%s,%d)", key, value)
		}
		seen[key] = true
		return true
	})
	if len(seen) != count {
		t.Fatalf("Range kept %d of %d entries alive", len(seen), count)
	}
}

func TestTypedConcurrentReplacement(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := smap.New[smap.Uint64Key, typedPayload](smap.UseEmbeddedPool[smap.Uint64Key, typedPayload](embedded), smap.MaxPefBucket[smap.Uint64Key, typedPayload](4))
			for k := smap.Uint64Key(0); k < 32; k++ {
				m.Set(k, typedValue(0))
			}
			var wg sync.WaitGroup
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						k := smap.Uint64Key(i % 32)
						if g%2 == 0 {
							if !m.Set(k, typedValue(i%200)) {
								t.Error("Set")
							}
							continue
						}
						value, ok := m.Get(k)
						if !ok {
							t.Error("continuous key missing")
							return
						}
						n := uint64(value.Tag)
						for _, word := range value.Words {
							if word != n {
								t.Error("torn struct")
								return
							}
						}
						if value.Text != fmt.Sprint(n) || len(value.Data) != 1 || uint64(value.Data[0]) != n || value.Ref == nil || uint64(*value.Ref) != n {
							t.Error("torn references")
							return
						}
					}
				}(g)
			}
			wg.Wait()
			if m.Len() != 32 {
				t.Fatal(m.Len())
			}
		})
	}
}
