//go:build stephook

package skiplistmap_test

import "testing"

// Keys a < b < c in a map without the embedded pool that never splits a
// bucket; the map holds all three, and Len is 3. G2 deletes b: LoadItem finds
// the item of b and Delete stops at delete.found, before it runs Delete of
// the item. The main goroutine deletes b to the end: it finds the same item,
// marks it deleted and runs AddLen(-1), so Len is 2. When G2 resumes, nothing
// looks at the item again: it marks the item deleted a second time, runs
// AddLen(-1) again and returns true. Len is then 1 while the map holds a and
// c, and both Deletes of the one key b return true.
func Test_J52ConcurrentDeletesOfOneKeyLowerLenTwice(t *testing.T) {
	keys := adjacentKeys(3)
	m := newStepMap()
	setKeys(t, m, keys)
	if n := m.base.Len(); n != 3 {
		t.Fatalf("Len = %d before the test, want 3", n)
	}

	s := newStepper(t)
	stop := s.stopAt("map.delete.found", nil)
	var ok1, ok2 bool
	done := goStep(t, func() { ok2 = m.Delete(keys[1]) })
	stop.waitReached(t, done)
	ok1 = m.Delete(keys[1])
	stop.Release()
	waitDone(t, done, "Delete(b) of G2")

	if _, ok := m.Get(keys[1]); ok {
		t.Errorf("Get(b) found after both Deletes")
	}
	for _, k := range []string{keys[0], keys[2]} {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
	if ok1 && ok2 {
		t.Errorf("both Deletes of the one key b returned true")
	}
	if n := m.base.Len(); n != 2 {
		t.Errorf("Len = %d after deleting b from a, b, c, want 2", n)
	}
}
