//go:build stephook

package skiplistmap_test

import "github.com/kazu/skiplistmap"

import (
	"testing"
	"unsafe"
)

func Test_EmbeddedDeleteDoesNotDeleteReusedSlot(t *testing.T) {
	for _, middle := range []bool{false, true} {
		name := "last"
		if middle {
			name = "middle"
		}
		t.Run(name, func(t *testing.T) {
			keys := adjacentKeys(3)
			m := newJ11Map(t).base
			if !m.Set(skiplistmap.StringKey(keys[0]), 10) {
				t.Fatal("initial Set failed")
			}
			if middle && !m.Set(skiplistmap.StringKey(keys[2]), 30) {
				t.Fatal("Set of the following key failed")
			}
			old, ok := m.LoadItemForTest(skiplistmap.StringKey(keys[0]))
			if !ok {
				t.Fatal("initial key missing")
			}
			s := newStepper(t)
			stop := s.stopAt("map.delete.found", isNode(unsafe.Pointer(old.PtrListHead())))
			var deleted bool
			done := goStep(t, func() { deleted = m.Delete(skiplistmap.StringKey(keys[0])) })
			stop.waitReached(t, done)
			if !m.Purge(skiplistmap.StringKey(keys[0])) {
				t.Fatal("Purge failed")
			}
			if !m.Set(skiplistmap.StringKey(keys[1]), 20) {
				t.Fatal("Set of the replacement key failed")
			}
			replacement, ok := m.LoadItemForTest(skiplistmap.StringKey(keys[1]))
			if !ok || replacement != old {
				t.Fatal("replacement did not reuse the purged slot")
			}
			stop.Release()
			waitDone(t, done, "Delete of the purged key")
			if deleted {
				t.Error("Delete returned true for the purged key")
			}
			if value, ok := m.Get(skiplistmap.StringKey(keys[1])); !ok || value != 20 {
				t.Errorf("replacement Get = (%v, %v), want (20, true)", value, ok)
			}
			want := 1
			if middle {
				want++
				if value, ok := m.Get(skiplistmap.StringKey(keys[2])); !ok || value != 30 {
					t.Errorf("following Get = (%v, %v), want (30, true)", value, ok)
				}
			}
			if got := m.Len(); got != want {
				t.Errorf("Len = %d, want %d", got, want)
			}
		})
	}
}

func Test_EmbeddedSearchDuringSlicePublication(t *testing.T) {
	keys := adjacentKeys(5)
	m := newJ11Map(t).base
	for _, i := range []int{1, 3, 4} {
		if !m.Set(skiplistmap.StringKey(keys[i]), i) {
			t.Fatal("initial Set failed")
		}
	}
	s := newStepper(t)
	// the writer has the new array ready and is about to publish it; the
	// reader takes its snapshot of the old one, the writer publishes, and
	// the reader goes on with the snapshot
	publish := s.stopAt("map.insertToPool.publish", nil)
	var inserted bool
	writer := goStep(t, func() { inserted = m.Set(skiplistmap.StringKey(keys[2]), 2) })
	publish.waitReached(t, writer)
	snapshot := s.stopAt("map.bsearch.snapshot", nil)
	var value interface{}
	var found bool
	reader := goStep(t, func() { value, found = m.Get(skiplistmap.StringKey(keys[4])) })
	snapshot.waitReached(t, reader)
	publish.Release()
	waitDone(t, writer, "Set during slice publication")
	snapshot.Release()
	waitDone(t, reader, "Get during slice publication")
	if !inserted {
		t.Error("Set failed")
	}
	if !found || value != 4 {
		t.Errorf("Get of an unchanged key = (%v, %v), want (4, true)", value, found)
	}
}

func Test_EmbeddedReuseDoesNotPublishPreviousValue(t *testing.T) {
	for _, tc := range []struct {
		name          string
		middle, purge bool
	}{
		{"purge/last", false, true},
		{"purge/middle", true, true},
		{"delete/last", false, false},
		{"delete/middle", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := adjacentKeys(3)
			m := newJ11Map(t).base
			if !m.Set(skiplistmap.StringKey(keys[0]), 10) || (tc.middle && !m.Set(skiplistmap.StringKey(keys[2]), 30)) {
				t.Fatal("initial Set failed")
			}
			old, ok := m.LoadItemForTest(skiplistmap.StringKey(keys[0]))
			if !ok {
				t.Fatal("initial key missing")
			}
			remove := m.Delete
			if tc.purge {
				remove = m.Purge
			}
			if !remove(skiplistmap.StringKey(keys[0])) {
				t.Fatal("removal failed")
			}
			s := newStepper(t)
			identity := s.stopAt("map.set.identity", isNode(unsafe.Pointer(old.PtrListHead())))
			var inserted bool
			writer := goStep(t, func() { inserted = m.Set(skiplistmap.StringKey(keys[1]), 20) })
			identity.waitReached(t, writer)
			if value, found := m.Get(skiplistmap.StringKey(keys[1])); found && value != 20 {
				t.Errorf("Get of the new key = %v during reuse, want not found or 20", value)
			}
			identity.Release()
			waitDone(t, writer, "Set of the replacement key")
			if !inserted {
				t.Fatal("Set failed")
			}
			if value, found := m.Get(skiplistmap.StringKey(keys[1])); !found || value != 20 {
				t.Errorf("Get after Set = (%v, %v), want (20, true)", value, found)
			}
		})
	}
}
