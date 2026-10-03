//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

func TestTypedAdjacentReplacements(t *testing.T) {
	m := newHashStepMap()
	left := newA8Item("left", 0x3000000000000001, 1)
	right := newA8Item("right", 0x3000000000000002, 1)
	m.StoreItem(left)
	m.StoreItem(right)
	s := newStepper(t)
	leftBeforeMark := s.stopAt("map.copy.replacement.inserted.marked", nil)
	rightMarked := s.stopAt("map.copy.replacement.inserted.marked", nil)
	leftDone := goStep(t, func() { m.Set(left.Key(), "updated-left") })
	leftBeforeMark.waitReached(t, leftDone)
	rightDone := goStep(t, func() { m.Set(right.Key(), "updated-right") })
	rightMarked.waitReached(t, rightDone)
	leftBeforeMark.Release()
	waitDone(t, leftDone, "left replacement")
	rightMarked.Release()
	waitDone(t, rightDone, "right replacement")
	for _, e := range []*a8Item{left, right} {
		if v, ok := m.Get(e.Key()); !ok || v != "updated-"+e.Key().name {
			t.Fatalf("Get(%s)=(%v,%v)", e.Key().name, v, ok)
		}
	}
	if m.Len() != 2 {
		t.Fatal(m.Len())
	}
	runtime.KeepAlive(left)
	runtime.KeepAlive(right)
}

func TestTypedValuesSkipsRetiredSlot(t *testing.T) {
	m := skiplistmap.New[skiplistmap.StringKey, int](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, int](true))
	m.Set("a", 6)
	m.Set("a", 7)
	s := newStepper(t)
	selected := s.stopAt("map.range.item", nil)
	done := goStep(t, func() {
		for value := range m.Values() {
			if value != 7 && value != 8 {
				t.Errorf("Values yielded unpublished value %d", value)
			}
		}
	})
	selected.waitReached(t, done)
	if !m.Set("a", 8) {
		t.Fatal("replacement failed")
	}
	selected.Release()
	waitDone(t, done, "Values")
}

func TestTypedDeleteWaitsForPoolReplacement(t *testing.T) {
	m := skiplistmap.New[skiplistmap.StringKey, int]()
	m.Set("a", 1)
	s := newStepper(t)
	updating := s.stopAt("map.copy.replacement.inserted.marked", nil)
	updated := goStep(t, func() { m.Set("a", 2) })
	updating.waitReached(t, updated)
	var deleted bool
	done := goStep(t, func() { deleted = m.Delete("a") })
	select {
	case <-done:
		t.Error("Delete finished while replacement owned the live entry")
	case <-time.After(100 * time.Millisecond):
	}
	updating.Release()
	waitDone(t, updated, "replacement")
	waitDone(t, done, "Delete")
	if !deleted {
		t.Fatal("Delete reported a live key absent")
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("deleted key is present")
	}
}

func TestTypedReplacementPrefersInsertedEntry(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, operation := range []string{"Set", "Update"} {
			t.Run(fmt.Sprintf("embedded=%v/%s", embedded, operation), func(t *testing.T) {
				m := skiplistmap.New[skiplistmap.StringKey, int](
					skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, int](embedded),
				)
				m.Set("a", 1)
				old, ok := m.LoadItemForTest("a")
				if !ok {
					t.Fatal("missing initial entry")
				}
				s := newStepper(t)
				inserted := s.stopAt("map.copy.replacement.inserted.marked", nil)
				done := goStep(t, func() {
					if operation == "Set" {
						m.Set("a", 2)
					} else {
						m.Update("a", func(value *int) { *value = 2 })
					}
				})
				inserted.waitReached(t, done)
				if !old.ListHead.IsMarked() {
					t.Error("old list node was not removed after the replacement was inserted")
				}
				type result struct {
					value int
					ok    bool
				}
				got := make(chan result, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					value, ok := m.Get("a")
					got <- result{value: value, ok: ok}
				}()
				<-started
				var current result
				readBeforeRelease := false
				select {
				case current = <-got:
					readBeforeRelease = true
				case <-time.After(50 * time.Millisecond):
				}
				inserted.Release()
				waitDone(t, done, "replacement")
				if !readBeforeRelease {
					select {
					case current = <-got:
					case <-time.After(time.Second):
						t.Fatal("Get did not finish after replacement")
					}
				}
				if !current.ok || current.value != 2 {
					t.Fatalf("Get=(%d,%v), want (2,true)", current.value, current.ok)
				}
				if value, ok := m.Get("a"); !ok || value != 2 {
					t.Fatalf("Get after replacement=(%d,%v), want (2,true)", value, ok)
				}
			})
		}
	}
}

// A split must finish preparing its child's pool before writers can acquire
// that child's independent mutex. Updates now allocate a replacement slot and
// can expand a pool, just as inserting a new key can.
func TestTypedSplitBlocksChildWriter(t *testing.T) {
	m := skiplistmap.New[skiplistmap.Uint64Key, int](
		skiplistmap.UseEmbeddedPool[skiplistmap.Uint64Key, int](true),
		skiplistmap.MaxPefBucket[skiplistmap.Uint64Key, int](4))
	s := newStepper(t)
	prepared := s.stopAt("map.makeBucket2.recurse", nil)
	done := goStep(t, func() {
		for i := uint64(1); i <= 256; i++ {
			m.Set(skiplistmap.Uint64Key(bits.Reverse64(i<<52)), int(i))
		}
	})
	prepared.waitReached(t, done)
	reverse := skiplistmap.StepBucketReverse[skiplistmap.Uint64Key, int](prepared.b) + 1
	key := skiplistmap.Uint64Key(bits.Reverse64(reverse))
	writing := s.stopAt("map.set.newKeyLock", nil)
	writerDone := goStep(t, func() { m.Set(key, -1) })
	writing.waitReached(t, writerDone)
	writing.Release()
	select {
	case <-writerDone:
		t.Error("writer changed child pool before its split finished")
	case <-time.After(100 * time.Millisecond):
	}
	prepared.Release()
	waitDone(t, done, "split and insertions")
	waitDone(t, writerDone, "child writer")
	for i := 0; i < 20; i++ {
		if !m.Set(key, i) {
			t.Fatal("replacement failed")
		}
	}
	if value, ok := m.Get(key); !ok || value != 19 {
		t.Fatalf("Get=(%d,%v), want (19,true)", value, ok)
	}
	for i := uint64(1); i <= 256; i++ {
		if value, ok := m.Get(skiplistmap.Uint64Key(bits.Reverse64(i << 52))); !ok || value != int(i) {
			t.Fatalf("original key %d: Get=(%d,%v)", i, value, ok)
		}
	}
}
