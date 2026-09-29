//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// D3: two StoreItem of one item a run at once. G1 stores a into m1 and stops
// at point, after it found a not linked; G2 stores a into m2, or into m1 too
// when same is set, and returns. When G1 goes on, it must return false, as
// it does when it runs after G2: a is linked by G2, and one item is linked
// into one list only. It returned true instead: at map.storeItem.checked,
// the lookup of the key found a itself and stored the value of a into it; at
// map.set.checked, the Init of a cut a out of the list of G2; at
// map.add2.found, add2 took a out of that list and linked it again; at
// elist.insert.begin, the insert took the links that G2 wrote as its own and
// relinked a, leaving the neighbors of a in that list leading to it.
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

func Test_D3StoreItemTwiceIntoOneMapBeforeTheLookup(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.storeItem.checked", true)
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
	done3 := goStep(t, func() { m2.base.Purge(keys[0]) })
	st3.waitReached(t, done3)
	st1.Release()
	waitDone(t, done1, "m1.StoreItem(a)")
	st3.Release()
	waitDone(t, done3, "m2.Purge(a)")

	assertStoredInOrder(t, m2, keys[1:])
	if ok1 {
		assertStoredInOrder(t, m1, keys)
	} else {
		assertStoredInOrder(t, m1, keys[1:])
	}
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
}

// Keys x < a < z; the map holds x and z. G1 stores a and stops after it
// linked a from x, before it links a from z. G2 deletes the key of a, or
// purges it when purge is set: it finds a and deletes it. G3 purges z and
// stops after it marked z, so that the insert of G1 fails and puts a back.
// G1 must not link a again: it returns true, as a was stored before G2
// deleted it, and a is gone. G1 linked a again when the delete of G2 left a
// linked, and returned false when the purge of G2 left its marks on a.
func testD3StoreItemKeepsADeleteBetweenTheCASesOfItsInsert(t *testing.T, purge bool) {
	keys := adjacentKeys(3)
	items := newStepItems(keys[1:2])
	a := &items[0]
	m := newStepMap()
	setKeys(t, m, []string{keys[0], keys[2]})
	zItem, ok := m.base.LoadItem(keys[2])
	if !ok {
		t.Fatalf("LoadItem(z) not found")
	}
	z := nodeOf(zItem.(*skiplistmap.SampleItem))

	s := newStepper(t)
	st1 := s.stopAt("elist.add.cas2", isNode(nodeOf(a)))
	done1 := goStep(t, func() { m.base.StoreItem(a) })
	st1.waitReached(t, done1)
	var done2 <-chan struct{}
	if purge {
		st2 := s.stopAt("elist.del.marked", isNode(nodeOf(a)))
		done2 = goStep(t, func() { m.base.Purge(keys[1]) })
		st2.waitReached(t, done2)
		st2.Release()
	} else {
		done2 = goStep(t, func() { m.base.Delete(keys[1]) })
		waitDone(t, done2, "Delete(a)")
	}
	st3 := s.stopAt("elist.del.marked", isNode(z))
	done3 := goStep(t, func() { m.base.Purge(keys[2]) })
	st3.waitReached(t, done3)
	back := s.stopAt("elist.add.rollback", isNode(nodeOf(a)))
	st1.Release()
	back.waitReached(t, done1)
	back.Release()
	st3.Release()
	waitDone(t, done1, "StoreItem(a)")
	waitDone(t, done2, "the delete of a")
	waitDone(t, done3, "Purge(z)")

	assertStoredInOrder(t, m, keys[:1])
}

func Test_D3StoreItemKeepsADeleteBetweenTheCASesOfItsInsert(t *testing.T) {
	testD3StoreItemKeepsADeleteBetweenTheCASesOfItsInsert(t, false)
}

func Test_D3StoreItemKeepsAPurgeBetweenTheCASesOfItsInsert(t *testing.T) {
	testD3StoreItemKeepsADeleteBetweenTheCASesOfItsInsert(t, true)
}
