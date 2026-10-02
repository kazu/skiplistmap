//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

func TestUpdateRetriesBeforeCallback(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, external := range []bool{false, true} {
			for _, operation := range []string{"Set", "Delete", "Purge", "Reuse", "Grow"} {
				t.Run(fmt.Sprintf("embedded=%v/external=%v/%s", embedded, external, operation), func(t *testing.T) {
					m := skiplistmap.New[skiplistmap.IntKey, int](
						skiplistmap.UseEmbeddedPool[skiplistmap.IntKey, int](embedded),
						skiplistmap.MaxPefBucket[skiplistmap.IntKey, int](8))
					e := skiplistmap.NewEntry[skiplistmap.IntKey, int](1, 7)
					defer runtime.KeepAlive(e)
					if external {
						m.StoreItem(e)
					} else {
						m.Set(1, 7)
					}
					old, _ := m.LoadItemForTest(1)
					s := newStepper(t)
					lookup := s.stopAt("map.update.lookup", isNode(nodeOf(old)))
					calls, seen := 0, 0
					var updated bool
					done := goStep(t, func() {
						updated = m.Update(1, func(v *int) { calls++; seen = *v; *v++ })
					})
					lookup.waitReached(t, done)
					want, present := 7, true
					switch operation {
					case "Set":
						if !m.Set(1, 20) {
							t.Fatal("Set")
						}
						want = 20
					case "Delete":
						if !m.Delete(1) {
							t.Fatal("Delete")
						}
						present = false
					case "Purge":
						if !m.Purge(1) {
							t.Fatal("Purge")
						}
						present = false
					case "Reuse":
						if !m.Purge(1) || !m.Set(1, 30) {
							t.Fatal("reuse")
						}
						want = 30
					case "Grow":
						for key := skiplistmap.IntKey(2); key < 98; key++ {
							if !m.Set(key, int(key)) {
								t.Fatal("growing Set")
							}
						}
					}
					lookup.Release()
					waitDone(t, done, "Update after replacement")
					if !present {
						if updated || calls != 0 {
							t.Fatalf("absent: updated=%v calls=%d", updated, calls)
						}
						return
					}
					if !updated || calls != 1 || seen != want {
						t.Fatalf("updated=%v calls=%d seen=%d want=%d", updated, calls, seen, want)
					}
					if got, ok := m.Get(1); !ok || got != want+1 {
						t.Fatalf("Get = %d, %v", got, ok)
					}
				})
			}
		}
	}
}

func TestUpdateAdjacentReplacement(t *testing.T) {
	m := newHashStepMap()
	left := newA8Item("left", 0x3000000000000001, 1)
	right := newA8Item("right", 0x3000000000000002, 1)
	m.StoreItem(left)
	m.StoreItem(right)
	defer runtime.KeepAlive(left)
	defer runtime.KeepAlive(right)
	s := newStepper(t)
	leftBeforeMark := s.stopAt("elist.replaceNode.mark", isNode(nodeOf(left)))
	rightMarked := s.stopAt("elist.replaceNode.marked", isNode(nodeOf(right)))
	calls := 0
	leftDone := goStep(t, func() {
		if !m.Update(left.Key(), func(v *any) { calls++; *v = "updated-left" }) {
			t.Error("Update")
		}
	})
	leftBeforeMark.waitReached(t, leftDone)
	rightDone := goStep(t, func() { m.Set(right.Key(), "updated-right") })
	rightMarked.waitReached(t, rightDone)
	leftBeforeMark.Release()
	waitDone(t, leftDone, "left Update")
	rightMarked.Release()
	waitDone(t, rightDone, "right Set")
	if calls != 1 {
		t.Fatalf("callback calls = %d", calls)
	}
	if v, ok := m.Get(left.Key()); !ok || v != "updated-left" {
		t.Fatalf("Get = %s, %v", v, ok)
	}
}
