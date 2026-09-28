//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
)

// D3: two StoreItem of one item a run at once. G1 stores a into m1 and stops
// at point, after it found a not linked; G2 stores a into m2, or into m1 too
// when same is set, and returns. When G1 goes on, it must return false, as
// it does when it runs after G2: a is linked by G2, and one item is linked
// into one list only. It returned true instead: at map.set.checked, the Init
// of a cut a out of the list of G2; at map.add2.found, add2 took a out of
// that list and linked it again; at elist.insert.begin, the insert took the
// links that G2 wrote as its own and relinked a, leaving the neighbors of a
// in that list leading to it.
func testD3StoreItemAtOnce(t *testing.T, point string, same bool) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[1:])
	setKeys(t, m2, keys[1:])
	if same {
		m2 = m1
	}

	s := newStepper(t)
	st := s.stopAt(point, isNode(nodeOf(a)))
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m1.base.StoreItem(a) })
	st.waitReached(t, done1)
	done2 := goStep(t, func() { ok2 = m2.base.StoreItem(a) })
	waitDone(t, done2, "the second StoreItem(a)")
	st.Release()
	waitDone(t, done1, "the first StoreItem(a)")

	if !ok2 || ok1 {
		t.Errorf("the first StoreItem(a) = %v and the second = %v, want false and true", ok1, ok2)
	}
	assertStoredInOrder(t, m2, keys)
	if !same {
		if _, ok := m1.Get(keys[0]); ok {
			t.Errorf("m1.Get(a) found, but a is linked into m2")
		}
		assertStoredInOrder(t, m1, keys[1:])
	}
	runtime.KeepAlive(items)
}

func Test_D3StoreItemIntoTwoMapsAfterTheCheck(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.set.checked", false)
}

func Test_D3StoreItemIntoTwoMapsAfterTheFind(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.add2.found", false)
}

func Test_D3StoreItemIntoTwoMapsBeforeTheLink(t *testing.T) {
	testD3StoreItemAtOnce(t, "elist.insert.begin", false)
}

func Test_D3StoreItemTwiceIntoOneMapAfterTheCheck(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.set.checked", true)
}

func Test_D3StoreItemTwiceIntoOneMapBeforeTheLink(t *testing.T) {
	testD3StoreItemAtOnce(t, "elist.insert.begin", true)
}

// D3: G1 stores a into m1 and stops after it found a not linked. G2 stores a
// into m2, and G3 purges the key of a from m2 and stops after it took a out
// of the list, before it clears the links of a. G1 goes on, and then G3. The
// list of m1 must stay whole, whether G1 stored a or refused it: G3 clears the
// links of a only while they keep the marks of its delete.
func Test_D3StoreItemWhileTheOtherMapPurgesIt(t *testing.T) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[1:])
	setKeys(t, m2, keys[1:])

	s := newStepper(t)
	st1 := s.stopAt("map.set.checked", isNode(nodeOf(a)))
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m1.base.StoreItem(a) })
	st1.waitReached(t, done1)
	if !m2.base.StoreItem(a) {
		t.Fatalf("m2.StoreItem(a) returned false")
	}
	st3 := s.stopAt("elist.del.check", isNode(nodeOf(a)))
	var ok3 bool
	done3 := goStep(t, func() { ok3 = m2.base.Purge(keys[0]) })
	st3.waitReached(t, done3)
	st1.Release()
	waitDone(t, done1, "m1.StoreItem(a)")
	st3.Release()
	waitDone(t, done3, "m2.Purge(a)")

	if !ok3 {
		t.Errorf("m2.Purge(a) returned false")
	}
	assertStoredInOrder(t, m2, keys[1:])
	if ok1 {
		assertStoredInOrder(t, m1, keys)
	} else {
		assertStoredInOrder(t, m1, keys[1:])
	}
	runtime.KeepAlive(items)
}

// D3: G1 stores a into m1 and stops after it found a not linked. G2 stores a
// into m2, and G3 deletes the key of a from m2; both return true. When G1
// goes on, it must not undo the Delete of G3: a stays deleted in m2.
func Test_D3StoreItemAfterTheCheckKeepsADeleteOfTheOtherMap(t *testing.T) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[1:])
	setKeys(t, m2, keys[1:])

	s := newStepper(t)
	st := s.stopAt("map.set.checked", isNode(nodeOf(a)))
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m1.base.StoreItem(a) })
	st.waitReached(t, done1)
	if !m2.base.StoreItem(a) {
		t.Fatalf("m2.StoreItem(a) returned false")
	}
	if !m2.base.Delete(keys[0]) {
		t.Fatalf("m2.Delete(a) returned false")
	}
	st.Release()
	waitDone(t, done1, "m1.StoreItem(a)")

	if ok1 {
		t.Errorf("m1.StoreItem(a) returned true, but a is linked into m2")
	}
	if _, ok := m2.Get(keys[0]); ok {
		t.Errorf("m2.Get(a) found after m2.Delete(a) returned true")
	}
	runtime.KeepAlive(items)
}
