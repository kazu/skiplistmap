//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"testing"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

func TestDeletePurgeDummyHashBounds(t *testing.T) {
	for _, reverse := range []uint64{0, 1, 1 << 60, 1<<60 + 1, ^uint64(0) - 1, ^uint64(0)} {
		for _, operation := range []string{"Delete", "Purge"} {
			t.Run(fmt.Sprintf("%x/%s", reverse, operation), func(t *testing.T) {
				m := newHashStepMap()
				items := make([]hashedItem, 3)
				for i := range items {
					items[i].InitEntry(fixedHashKey{fmt.Sprint(i), bits.Reverse64(reverse), 17}, i)
					if !m.StoreItem(&items[i]) {
						t.Fatal("StoreItem failed")
					}
				}
				for n, i := range []int{1, 0, 2} {
					var ok bool
					if operation == "Delete" {
						ok = m.Delete(items[i].Key())
					} else {
						ok = m.Purge(items[i].Key())
					}
					if !ok || m.Len() != 2-n {
						t.Fatalf("%s(%d) = %v, Len = %d", operation, i, ok, m.Len())
					}
					if _, found := m.Get(items[i].Key()); found {
						t.Fatal("deleted key remains present")
					}
				}
				runtime.KeepAlive(items)
			})
		}
	}
}

func TestDeletePurgeDummyAfterSplit(t *testing.T) {
	for _, operation := range []string{"Delete", "Purge"} {
		t.Run(operation, func(t *testing.T) {
			keys := adjacentKeys(256)
			m := newStepMap().base
			skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)(m)
			items := newStepItems(keys[:1])
			entry := &items[0]
			if !m.StoreItem(entry) {
				t.Fatal("StoreItem failed")
			}
			s := newStepper(t)
			st := s.stopAt("map.delete.found", isNode(nodeOf(entry)))
			defer st.Release()
			var deleted bool
			done := goStep(t, func() {
				if operation == "Delete" {
					deleted = m.Delete(entry.Key())
				} else {
					deleted = m.Purge(entry.Key())
				}
			})
			st.waitReached(t, done)
			for i := 1; i < len(keys); i++ {
				if !m.Set(skiplistmap.StringKey(keys[i]), i) {
					t.Fatal("Set failed")
				}
			}
			if s.total("map.insertBucket.dummyLinked") == 0 {
				t.Fatal("test did not insert a bucket dummy")
			}
			st.Release()
			waitDone(t, done, "deletion after split")
			if !deleted || m.Len() != len(keys)-1 {
				t.Fatalf("deleted = %v, Len = %d", deleted, m.Len())
			}
			if _, found := m.Get(entry.Key()); found {
				t.Fatal("deleted key remains present")
			}
			runtime.KeepAlive(items)
		})
	}
}

func TestDeletePurgeDummyCursorPurged(t *testing.T) {
	for _, operation := range []string{"Delete", "Purge"} {
		t.Run(operation, func(t *testing.T) {
			keys := adjacentKeys(3)
			items := newStepItems(keys)
			m := newStepMap().base
			for i := range items {
				if !m.StoreItem(&items[i]) {
					t.Fatal("StoreItem failed")
				}
			}
			s := newStepper(t)
			st := s.stopAt("map.delete.scan", func(a, b, _ unsafe.Pointer) bool {
				return b == nodeOf(&items[1]) && (a == nodeOf(&items[0]) || a == nodeOf(&items[2]))
			})
			defer st.Release()
			var deleted bool
			done := goStep(t, func() {
				if operation == "Delete" {
					deleted = m.Delete(items[1].Key())
				} else {
					deleted = m.Purge(items[1].Key())
				}
			})
			st.waitReached(t, done)
			cursor, _ := s.args("map.delete.scan")
			for _, i := range []int{0, 2} {
				if cursor == nodeOf(&items[i]) && !m.Purge(items[i].Key()) {
					t.Fatal("cursor Purge failed")
				}
			}
			st.Release()
			waitDone(t, done, "deletion after cursor Purge")
			if !deleted || m.Len() != 1 {
				t.Fatalf("deleted = %v, Len = %d", deleted, m.Len())
			}
			runtime.KeepAlive(items)
		})
	}
}

func Test_F2DeleteStoppedAfterItsLookupLeavesTheItemStoredIntoAnotherMap(t *testing.T) {
	keys := adjacentKeys(1)
	items := newStepItems(keys)
	u := &items[0]
	key := skiplistmap.StringKey(keys[0])
	m0, m1 := newStepMap(), newStepMap()
	if !m0.base.StoreItem(u) {
		t.Fatal("m0.StoreItem(u) returned false")
	}
	s := newStepper(t)
	st := s.stopAt("map.delete.found", isNode(nodeOf(u)))
	var deleted bool
	done := goStep(t, func() { deleted = m0.base.Delete(key) })
	st.waitReached(t, done)
	if !m0.base.Purge(key) {
		t.Fatal("m0.Purge(k) returned false")
	}
	if !m1.base.StoreItem(u) {
		t.Fatal("m1.StoreItem(u) returned false")
	}
	st.Release()
	waitDone(t, done, "m0.Delete(k)")
	if deleted {
		t.Error("m0.Purge(k) and m0.Delete(k) both returned true")
	}
	if got := m0.base.Len(); got != 0 {
		t.Errorf("m0.Len() = %d, want 0", got)
	}
	if _, ok := m1.Get(keys[0]); !ok {
		t.Error("m1.Get(k) not found after m1.StoreItem(u) returned true")
	}
	if got := m1.base.Len(); got != 1 {
		t.Errorf("m1.Len() = %d, want 1", got)
	}
	runtime.KeepAlive(items)
}

func TestDeletePurgeAfterConcurrentDelete(t *testing.T) {
	for _, operation := range []string{"Delete", "Purge"} {
		t.Run(operation, func(t *testing.T) {
			m := newStepMap().base
			key := skiplistmap.StringKey("key")
			if !m.Set(key, 1) {
				t.Fatal("initial Set failed")
			}
			entry, ok := m.LoadItem(key)
			if !ok {
				t.Fatal("initial key missing")
			}
			s := newStepper(t)
			found := s.stopAt("map.delete.found", isNode(unsafe.Pointer(entry.PtrListHead())))
			defer found.Release()
			var stale bool
			doneStale := goStep(t, func() {
				if operation == "Delete" {
					stale = m.Delete(key)
				} else {
					stale = m.Purge(key)
				}
			})
			found.waitReached(t, doneStale)
			claimed := s.stopAt("map.delete.claimed", isNode(unsafe.Pointer(entry.PtrListHead())))
			defer claimed.Release()
			var winner bool
			doneWinner := goStep(t, func() { winner = m.Delete(key) })
			claimed.waitReached(t, doneWinner)
			found.Release()
			waitDone(t, doneStale, "stale deletion while winner holds mapIsBusy")
			if stale {
				t.Error("both deletions succeeded")
			}
			claimed.Release()
			waitDone(t, doneWinner, "winning deletion")
			if !winner || m.Len() != 0 {
				t.Errorf("winner = %v, Len = %d", winner, m.Len())
			}
		})
	}
}

func TestDeletePurgeAfterEntryReuse(t *testing.T) {
	for _, operation := range []string{"Delete", "Purge"} {
		for _, destination := range []string{"detached", "same", "other", "other-with-replacement"} {
			t.Run(operation+"/"+destination, func(t *testing.T) {
				keys := adjacentKeys(1)
				items := newStepItems(keys)
				replacements := newStepItems(keys)
				key := skiplistmap.StringKey(keys[0])
				m0, m1 := newStepMap(), newStepMap()
				if !m0.base.StoreItem(&items[0]) {
					t.Fatal("initial StoreItem failed")
				}
				s := newStepper(t)
				st := s.stopAt("map.delete.found", isNode(nodeOf(&items[0])))
				var deleted bool
				done := goStep(t, func() {
					if operation == "Delete" {
						deleted = m0.base.Delete(key)
					} else {
						deleted = m0.base.Purge(key)
					}
				})
				st.waitReached(t, done)
				if !m0.base.Purge(key) {
					t.Fatal("Purge before reuse failed")
				}
				switch destination {
				case "same":
					if !m0.base.StoreItem(&items[0]) {
						t.Fatal("same-map StoreItem failed")
					}
				case "other", "other-with-replacement":
					if !m1.base.StoreItem(&items[0]) {
						t.Fatal("other-map StoreItem failed")
					}
					if destination == "other-with-replacement" && !m0.base.StoreItem(&replacements[0]) {
						t.Fatal("replacement StoreItem failed")
					}
				}
				st.Release()
				waitDone(t, done, operation)
				if deleted != (destination == "same") {
					t.Errorf("%s returned %v", operation, deleted)
				}
				want0, want1 := 0, 0
				if destination == "other-with-replacement" {
					want0 = 1
				}
				if destination == "other" || destination == "other-with-replacement" {
					want1 = 1
				}
				for i, m := range []*WrapHMap{m0, m1} {
					want := []int{want0, want1}[i]
					if got := m.base.Len(); got != want {
						t.Errorf("m%d.Len() = %d, want %d", i, got, want)
					}
					if _, found := m.Get(keys[0]); found != (want == 1) {
						t.Errorf("m%d.Get() found = %v, want %v", i, found, want == 1)
					}
				}
				runtime.KeepAlive(items)
				runtime.KeepAlive(replacements)
			})
		}
	}
}
