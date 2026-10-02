//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"unsafe"

	smap "github.com/kazu/skiplistmap"
)

func TestEntryCopyRetiredAfterStoreCheck(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, copyRetired := range []bool{false, true} {
			a := smap.New[smap.StringKey, any](smap.UseEmbeddedPool[smap.StringKey, any](embedded))
			b := smap.New[smap.StringKey, any](smap.UseEmbeddedPool[smap.StringKey, any](embedded))
			e := smap.NewEntry[smap.StringKey, any]("key", 1)
			s := newStepper(t)
			st := s.stopAt("map.storeItem.checked", isNode(unsafe.Pointer(e.PtrListHead())))
			var stored bool
			var fresh *smap.Entry[smap.StringKey, any]
			done := goStep(t, func() {
				if copyRetired {
					fresh, stored = b.StoreItemOrCopy(e)
				} else {
					stored = b.StoreItem(e)
				}
			})
			st.waitReached(t, done)
			if !a.StoreItem(e) || !a.Purge("key") {
				st.Release()
				t.Fatal("concurrent StoreItem/Purge failed")
			}
			st.Release()
			waitDone(t, done, "StoreItem after concurrent retirement")
			if stored != copyRetired || a.Len() != 0 {
				t.Fatalf("stored=%v, copy=%v, A.Len=%d", stored, copyRetired, a.Len())
			}
			if copyRetired {
				if fresh == nil || fresh == e || b.Len() != 1 {
					t.Fatal("retired entry was not copied")
				}
			} else if b.Len() != 0 {
				t.Fatal("refused entry changed destination")
			}
			runtime.KeepAlive(e)
			runtime.KeepAlive(fresh)
			smap.SetStepHook(nil)
		}
	}
}

func TestEntryCopyRetiredBusyRefused(t *testing.T) {
	m := newStepMap().base
	e := smap.NewEntry[smap.StringKey, any]("key", 1)
	if !m.StoreItem(e) {
		t.Fatal("StoreItem failed")
	}
	s := newStepper(t)
	st := s.stopAt("map.delete.claimed", isNode(unsafe.Pointer(e.PtrListHead())))
	defer st.Release()
	done := goStep(t, func() { m.Purge("key") })
	st.waitReached(t, done)
	fresh, ok := m.StoreItemOrCopy(e)
	st.Release()
	waitDone(t, done, "Purge")
	if ok || fresh != nil || m.Len() != 0 {
		t.Fatal("busy retired entry was copied")
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyRetirementDoesNotHideLiveKey(t *testing.T) {
	m := smap.New[smap.StringKey, any](smap.UseEmbeddedPool[smap.StringKey, any](true))
	e := smap.NewEntry[smap.StringKey, any]("key", 1)
	if !m.StoreItem(e) {
		t.Fatal("StoreItem failed")
	}
	s := newStepper(t)
	read := s.stopAt("map.key.dataRead", isNode(unsafe.Pointer(e.PtrListHead())))
	defer read.Release()
	var got *smap.Entry[smap.StringKey, any]
	var found bool
	lookup := goStep(t, func() { got, found = m.LoadItemForTest("key") })
	read.waitReached(t, lookup)
	mark := s.stopAt("map.copy.replacement.inserted.marked", nil)
	defer mark.Release()
	var updated bool
	writer := goStep(t, func() { updated = m.Set("key", 2) })
	mark.waitReached(t, writer)
	read.Release()
	waitDone(t, lookup, "lookup before replacement marks")
	mark.Release()
	waitDone(t, writer, "replacement")
	if !found || got != e || !updated {
		t.Fatalf("found=%v, entry=%p (want %p), updated=%v", found, got, e, updated)
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyMarkedReplacementRefusesReuse(t *testing.T) {
	m := newStepMap().base
	e := smap.NewEntry[smap.StringKey, any]("key", 1)
	if !m.StoreItem(e) {
		t.Fatal("StoreItem failed")
	}
	s := newStepper(t)
	mark := s.stopAt("map.copy.replacement.inserted.marked", nil)
	defer mark.Release()
	done := goStep(t, func() { m.Set("key", 2) })
	mark.waitReached(t, done)
	other := newStepMap().base
	if other.StoreItem(e) {
		t.Error("marked busy entry was reused")
	}
	if fresh, ok := other.StoreItemOrCopy(e); ok || fresh != nil {
		t.Error("busy replacement was copied")
	}
	mark.Release()
	waitDone(t, done, "replacement")
	e.PtrListHead().InitMarked()
	if other.StoreItem(e) {
		t.Error("completed replacement lost its history")
	}
	fresh, ok := other.StoreItemOrCopy(e)
	if !ok || fresh == nil || fresh == e || fresh.Value() != 1 {
		t.Error("completed replacement could not be copied")
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(fresh)
}

func TestDeletePurgeDummyCursorCopied(t *testing.T) {
	for _, operation := range []string{"Delete", "Purge"} {
		t.Run(operation, func(t *testing.T) {
			items := newStepItems(adjacentKeys(3))
			a, b := newStepMap().base, newStepMap().base
			for i := range items {
				if !a.StoreItem(&items[i]) {
					t.Fatal("initial StoreItem")
				}
			}
			s := newStepper(t)
			found := s.stopAt("map.delete.found", isNode(nodeOf(&items[1])))
			defer found.Release()
			var deleted bool
			done := goStep(t, func() {
				if operation == "Delete" {
					deleted = a.Delete(items[1].Key())
				} else {
					deleted = a.Purge(items[1].Key())
				}
			})
			found.waitReached(t, done)
			if !a.Purge(items[1].Key()) || b.StoreItem(&items[1]) {
				t.Fatal("retired target was not refused")
			}
			target := items[1].Copy()
			if !b.StoreItem(target) {
				t.Fatal("target copy")
			}
			// The old target remains deleted, so a fresh stale deletion refuses
			// it before a membership scan. Exercise a cursor that was already
			// acquired by another deletion instead.
			found.Release()
			waitDone(t, done, "stale deletion after target copy")
			if deleted || a.Len() != 2 || b.Len() != 1 {
				t.Fatalf("deleted=%v, A.Len=%d, B.Len=%d", deleted, a.Len(), b.Len())
			}
			scan := s.stopAt("map.delete.scan", func(x, y, _ unsafe.Pointer) bool {
				return y == nodeOf(&items[2]) && x == nodeOf(&items[0])
			})
			defer scan.Release()
			done = goStep(t, func() { deleted = a.Delete(items[2].Key()) })
			scan.waitReached(t, done)
			if !a.Purge(items[0].Key()) || b.StoreItem(&items[0]) {
				t.Fatal("retired cursor was not refused")
			}
			cursor := items[0].Copy()
			if !b.StoreItem(cursor) {
				t.Fatal("cursor copy")
			}
			scan.Release()
			waitDone(t, done, "deletion after cursor copy")
			if !deleted || a.Len() != 0 || b.Len() != 2 {
				t.Errorf("deleted=%v, A.Len=%d, B.Len=%d", deleted, a.Len(), b.Len())
			}
			if _, ok := b.Get(target.Key()); !ok {
				t.Error("target disappeared from B")
			}
			runtime.KeepAlive(items)
			runtime.KeepAlive(target)
			runtime.KeepAlive(cursor)
		})
	}
}
