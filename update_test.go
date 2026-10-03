package skiplistmap_test

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kazu/skiplistmap"
)

type updateKey int

func (updateKey) KeyHash() (uint64, uint64)    { return 17, 23 }
func (k updateKey) Equal(other updateKey) bool { return k == other }

func TestUpdate(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := skiplistmap.New[updateKey, int](skiplistmap.UseEmbeddedPool[updateKey, int](embedded))
			if m.Update(1, func(*int) { t.Error("empty Map called edit") }) {
				t.Fatal("empty Map Update")
			}
			external := skiplistmap.NewEntry[updateKey, int](2, 20)
			defer runtime.KeepAlive(external)
			if !m.Set(1, 10) || !m.StoreItem(external) {
				t.Fatal("setup")
			}
			calls := 0
			for _, key := range []updateKey{1, 2} {
				if !m.Update(key, func(v *int) { calls++; *v += 3 }) {
					t.Fatal("present key", key)
				}
				if got, ok := m.Get(key); !ok || got != int(key)*10+3 {
					t.Fatalf("Get(%v) = %v, %v", key, got, ok)
				}
			}
			if m.Update(3, func(*int) { calls++ }) || calls != 2 {
				t.Fatalf("callback calls = %d", calls)
			}
			if external.Value() != 20 {
				t.Fatal("modified the old external Entry")
			}
		})
	}
}

func TestUpdateConcurrent(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("embedded=%v/external=%v", embedded, external), func(t *testing.T) {
				m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
				e := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 0)
				defer runtime.KeepAlive(e)
				if external {
					m.StoreItem(e)
				} else {
					m.Set(1, 0)
				}
				var wg sync.WaitGroup
				var successes atomic.Int32
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 100 {
							calls := 0
							updated := m.Update(1, func(v *int) { calls++; *v++ })
							if (updated && calls != 1) || (!updated && calls != 0) {
								t.Errorf("updated=%v calls=%d", updated, calls)
							}
							if updated {
								successes.Add(1)
							}
						}
					}()
				}
				wg.Wait()
				if v, ok := m.Get(1); !ok || v != int(successes.Load()) {
					t.Fatalf("value=%d present=%v successful updates=%d", v, ok, successes.Load())
				}
			})
		}
	}
}

func TestUpdatePanic(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("embedded=%v/external=%v", embedded, external), func(t *testing.T) {
				m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
				e := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 0)
				defer runtime.KeepAlive(e)
				if external {
					m.StoreItem(e)
				} else {
					m.Set(1, 0)
				}
				for _, key := range []skiplistmap.IntKey{1, 2} {
					func() {
						defer func() {
							if recover() == nil {
								t.Error("nil callback did not panic")
							}
						}()
						m.Update(key, nil)
					}()
				}
				func() {
					defer func() {
						if got := recover(); got != "edit" {
							t.Errorf("panic = %v", got)
						}
					}()
					m.Update(1, func(v *int) { *v = 99; panic("edit") })
				}()
				if !m.Set(1, 10) || !m.Update(1, func(v *int) { *v++ }) {
					t.Fatal("protection was not released")
				}
				if got, ok := m.Get(1); !ok || got != 11 {
					t.Fatalf("Get = %d, %v", got, ok)
				}
				if !m.Purge(1) || !m.Set(1, 12) {
					t.Fatal("reuse after panic")
				}
			})
		}
	}
}

func ExampleMap_Update() {
	m := skiplistmap.New[skiplistmap.StringKey, int]()
	m.Set("count", 3)
	found := m.Update("count", func(value *int) { *value += 2 })
	value, _ := m.Get("count")
	fmt.Println(found, value)
	// Output: true 5
}

func TestUpdateValues(t *testing.T) {
	type largeValue struct {
		Words [128]uint64
		Ref   *int
		Bytes []byte
	}
	x := 7
	value := largeValue{Ref: &x, Bytes: []byte{1, 2}}
	value.Words[127] = 99
	testUpdateValue(t, "zero", 0, 11)
	testUpdateValue(t, "large", largeValue{}, value)
	testUpdateValue(t, "pointer", (*largeValue)(nil), &value)
	testUpdateValue(t, "slice", []byte(nil), value.Bytes)
	testUpdateValue[any](t, "interface", nil, value)
}

func testUpdateValue[V any](t *testing.T, name string, before, after V) {
	t.Helper()
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s/%v", name, embedded), func(t *testing.T) {
			m := skiplistmap.New[skiplistmap.IntKey, V](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, V](embedded))
			m.Set(1, before)
			if !m.Update(1, func(v *V) {
				if !reflect.DeepEqual(*v, before) {
					t.Fatal("wrong current value")
				}
				*v = after
			}) {
				t.Fatal("Update")
			}
			got, ok := m.Get(1)
			if !ok || !reflect.DeepEqual(got, after) {
				t.Fatalf("Get = %#v, %v", got, ok)
			}
		})
	}
}

func TestUpdateWhileGrowing(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := skiplistmap.New[skiplistmap.IntKey, int](
				skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded),
				skiplistmap.MaxPefBucket[skiplistmap.IntKey, int](8))
			entry := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 0)
			defer runtime.KeepAlive(entry)
			m.StoreItem(entry)
			m.Set(2, 0)
			var wg sync.WaitGroup
			var successes [2]atomic.Int32
			for _, key := range []skiplistmap.IntKey{1, 2} {
				for range 2 {
					wg.Add(1)
					go func(key skiplistmap.IntKey) {
						defer wg.Done()
						for range 100 {
							calls := 0
							updated := m.Update(key, func(v *int) { calls++; *v++ })
							if (updated && calls != 1) || (!updated && calls != 0) {
								t.Errorf("key=%d updated=%v calls=%d", key, updated, calls)
							}
							if updated {
								successes[key-1].Add(1)
							}
						}
					}(key)
				}
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for key := skiplistmap.IntKey(3); key < 131; key++ {
					if !m.Set(key, int(key)) {
						t.Error("growing Set")
						return
					}
				}
				for key := skiplistmap.IntKey(3); key < 131; key++ {
					if !m.Purge(key) || !m.Set(key, int(key)+1) {
						t.Error("slot reuse")
						return
					}
				}
			}()
			wg.Wait()
			for _, key := range []skiplistmap.IntKey{1, 2} {
				want := int(successes[key-1].Load())
				if got, ok := m.Get(key); !ok || got != want {
					t.Fatalf("key %d = %d, %v; successful updates=%d", key, got, ok, want)
				}
			}
			for key := skiplistmap.IntKey(3); key < 131; key++ {
				if got, ok := m.Get(key); !ok || got != int(key)+1 {
					t.Fatalf("key %d = %d, %v", key, got, ok)
				}
			}
		})
	}
}

func TestUpdateReadPublication(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprint(embedded), func(t *testing.T) {
			m := skiplistmap.New[skiplistmap.IntKey, [32]int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, [32]int](embedded))
			m.Set(1, [32]int{})
			var done atomic.Bool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !done.Load() {
					v, ok := m.Get(1)
					if !ok {
						t.Error("key disappeared")
						return
					}
					for _, word := range v {
						if word != v[0] {
							t.Error("partially edited value")
							return
						}
					}
				}
			}()
			for range 500 {
				if !m.Update(1, func(v *[32]int) {
					v[0]++
					runtime.Gosched()
					for i := 1; i < len(v); i++ {
						v[i] = v[0]
					}
				}) {
					t.Error("Update")
					break
				}
			}
			done.Store(true)
			wg.Wait()
		})
	}
}

func TestUpdateConcurrentSetDelete(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("embedded=%v/external=%v", embedded, external), func(t *testing.T) {
				m := skiplistmap.New[skiplistmap.IntKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded))
				e := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 0)
				defer runtime.KeepAlive(e)
				if external {
					m.StoreItem(e)
				} else {
					m.Set(1, 0)
				}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						m.Set(1, i)
						if i%2 == 0 {
							m.Delete(1)
						} else {
							m.Purge(1)
						}
					}
				}()
				for range 100 {
					calls := 0
					updated := m.Update(1, func(v *int) { calls++; *v++ })
					if (updated && calls != 1) || (!updated && calls != 0) {
						t.Errorf("updated=%v calls=%d", updated, calls)
					}
				}
				wg.Wait()
				if !m.Set(1, 100) || !m.Update(1, func(v *int) { *v++ }) {
					t.Fatal("final update")
				}
				if got, ok := m.Get(1); !ok || got != 101 {
					t.Fatalf("Get = %d, %v", got, ok)
				}
			})
		}
	}
}
