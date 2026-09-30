//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

func Test_OperationsDistinguishSameHashPair(t *testing.T) {
	const key = "wanted"
	hash, conflict := skiplistmap.KeyToHash(key)
	items := make([]hashedItem, 2)
	for i, stored := range []string{"other", key} {
		items[i].K = stored
		items[i].k, items[i].conflict = hash, conflict
		items[i].SetValue(i)
	}
	m := newStepMap().base
	m.StoreItem(&items[0])
	if value, ok := m.Get(key); ok {
		t.Errorf("Get returned colliding other key: %v", value)
	}
	if _, ok := m.LoadItem(key); ok || m.Delete(key) || m.Purge(key) {
		t.Fatal("an operation accepted the colliding other key")
	}
	m.StoreItem(&items[1])
	for _, lookup := range []interface{}{key, []byte(key)} {
		if value, ok := m.Get(lookup); !ok || value != 1 {
			t.Errorf("Get(%v)=(%v,%v), want (1,true)", lookup, value, ok)
		}
	}
	if !m.Set(key, 2) {
		t.Fatal("Set of the actual key failed")
	}
	if items[0].Value() != 0 || items[1].Value() != 2 || m.Len() != 2 {
		t.Fatalf("Set changed the wrong entry: other=%v wanted=%v Len=%d", items[0].Value(), items[1].Value(), m.Len())
	}
	if !m.Delete(key) || m.Delete(key) {
		t.Fatal("Delete did not count the actual key exactly once")
	}
	remaining, ok := m.LoadItemByHash(hash, conflict)
	if !ok || remaining.Key() != "other" || remaining.Value() != 0 || m.Len() != 1 {
		t.Fatalf("Delete lost the colliding entry: %v, %v, Len=%d", remaining, ok, m.Len())
	}
	runtime.KeepAlive(items)
}

func Test_EmbeddedGetDoesNotReadReusedSlot(t *testing.T) {
	for _, point := range []string{"map.get.beforeValue", "map.key.dataRead"} {
		for _, byHash := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/byHash=%v", point, byHash), func(t *testing.T) {
				testEmbeddedGetReusedSlot(t, point, byHash)
			})
		}
	}
}

func testEmbeddedGetReusedSlot(t *testing.T, point string, byHash bool) {
	keys := adjacentKeys(2)
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("a-longer-replacement-key-%d", i)
		if reverseOf(candidate)>>60 == reverseOf(keys[0])>>60 {
			keys[1] = candidate
			break
		}
	}
	m := newJ11Map(t).base
	if !m.Set(keys[0], 10) {
		t.Fatal("initial Set failed")
	}
	old, ok := m.LoadItem(keys[0])
	if !ok {
		t.Fatal("initial key missing")
	}
	s := newStepper(t)
	stop := s.stopAt(point, isNode(unsafe.Pointer(old.PtrListHead())))
	var value interface{}
	var found bool
	hash, conflict := skiplistmap.KeyToHash(keys[0])
	done := goStep(t, func() {
		if byHash {
			value, found = m.GetByHash(hash, conflict)
		} else {
			value, found = m.Get(keys[0])
		}
	})
	stop.waitReached(t, done)
	if !m.Purge(keys[0]) || !m.Set(keys[1], 20) {
		t.Fatal("slot reuse failed")
	}
	replacement, ok := m.LoadItem(keys[1])
	if !ok || replacement != old {
		t.Fatal("replacement did not reuse the slot")
	}
	stop.Release()
	waitDone(t, done, "Get of the purged key")
	if found && value != 10 {
		t.Errorf("Get of old key returned (%v, %v), want old value or not found", value, found)
	}
}

func Test_EmbeddedRangeKeepsKeyAndValueTogether(t *testing.T) {
	keys := adjacentKeys(2)
	m := newJ11Map(t).base
	m.Set(keys[0], 10)
	s := newStepper(t)
	stop := s.stopAt("map.range.keyRead", nil)
	var gotKey, gotValue interface{}
	done := goStep(t, func() {
		m.Range(func(key, value interface{}) bool {
			gotKey, gotValue = key, value
			return false
		})
	})
	stop.waitReached(t, done)
	if !m.Purge(keys[0]) || !m.Set(keys[1], 20) {
		t.Fatal("slot reuse failed")
	}
	stop.Release()
	waitDone(t, done, "Range")
	if (gotKey == keys[0] && gotValue != 10) || (gotKey == keys[1] && gotValue != 20) {
		t.Errorf("Range combined different entries: (%v,%v)", gotKey, gotValue)
	}
}

func Test_DifferentKeysWithSameHashPair(t *testing.T) {
	for _, mode := range []string{"sequential", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			items := make([]hashedItem, 2)
			for i, key := range []string{"first", "second"} {
				items[i].K = key
				items[i].k, items[i].conflict = 0x0123456789abcdef, 1
				items[i].SetValue(i)
			}
			m := newStepMap()
			if mode == "concurrent" {
				runTogether(len(items), func(i int) { m.base.StoreItem(&items[i]) })
			} else {
				m.base.StoreItem(&items[0])
				m.base.StoreItem(&items[1])
			}
			keys := make(map[interface{}]interface{})
			m.base.Range(func(key, value interface{}) bool {
				keys[key] = value
				return true
			})
			if len(keys) != 2 || keys["first"] != 0 || keys["second"] != 1 {
				t.Errorf("different keys with the same hash pair: %v, want first=0 and second=1", keys)
			}
			runtime.KeepAlive(items)
		})
	}
}

func Test_ConcurrentSetSameNewKey(t *testing.T) {
	m := newPoolMap(32)
	if !m.base.Set("sentinel", 0) {
		t.Fatal("initial Set failed")
	}
	s := newStepper(t)
	stop := s.stopAt("map.set.beforeInit", nil)
	done := goStep(t, func() {
		if !m.base.Set("same", 1) {
			t.Error("first Set failed")
		}
	})
	stop.waitReached(t, done)
	if !m.base.Set("same", 2) {
		t.Fatal("second Set failed")
	}
	stop.Release()
	waitDone(t, done, "Set(same)")
	count := 0
	m.base.Range(func(key, value interface{}) bool {
		if key == "same" {
			count++
		}
		return true
	})
	if count != 1 || m.base.Len() != 2 {
		t.Fatalf("same key appears %d times; Len=%d, want 1 and 2", count, m.base.Len())
	}
}
