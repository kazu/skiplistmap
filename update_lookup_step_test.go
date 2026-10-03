//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

func TestUpdateLookupFailureAfterSlotReuse(t *testing.T) {
	m := skiplistmap.New[skiplistmap.IntKey, int](
		skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](true),
		skiplistmap.MaxPefBucket[skiplistmap.IntKey, int](8))
	if !m.Set(2, 0) || !m.Set(2, 0) {
		t.Fatal("initial Set")
	}
	old, _ := m.LoadItemForTest(2)
	s := newStepper(t)
	stop := s.stopAt("map.key.dataRead", isNode(nodeOf(old)))
	calls := 0
	var updated bool
	done := goStep(t, func() {
		updated = m.Update(2, func(v *int) { calls++; *v++ })
	})
	stop.waitReached(t, done)
	for i := 0; i < 2; i++ {
		if !m.Update(2, func(v *int) { *v++ }) {
			t.Fatal("concurrent Update")
		}
	}
	current, _ := m.LoadItemForTest(2)
	if current != old {
		t.Fatal("fixture did not reuse the candidate slot")
	}
	stop.Release()
	waitDone(t, done, "Update after candidate reuse")
	value, found := m.Get(2)
	if updated || calls != 0 {
		t.Fatalf("Update=%v callback calls=%d, want false and 0", updated, calls)
	}
	if !found || value != 2 {
		t.Fatalf("Get=(%d,%v), want (2,true)", value, found)
	}
	if !m.Update(2, func(v *int) { *v++ }) {
		t.Fatal("Update after concurrent operations finished")
	}
	if value, found := m.Get(2); !found || value != 3 {
		t.Fatalf("Get after final Update=(%d,%v), want (3,true)", value, found)
	}
}
