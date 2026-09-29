//go:build stephook

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// D3: two StoreItem of one item a run at once. G1 stores a into m1 and stops
// at point, after it found a not linked; G2 stores a into m2, or into m1 too
// when same is set, and returns. One item is linked into one list only, so
// one of them must return false. When busy is not set, G1 stops before it
// takes the mapIsBusy of a: G2 links a, and when G1 goes on, it must return
// false, as it does when it runs after G2. When busy is set, G1 stops holding
// the mapIsBusy of a: G2 must return false, and G1 links a when it goes on.
func testD3StoreItemAtOnce(t *testing.T, point string, same, busy bool) {
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

	holder, other := m2, m1
	if busy {
		holder, other = m1, m2
	}
	if ok1 != busy || ok2 == busy {
		t.Errorf("the first StoreItem(a) = %v and the second = %v, want %v and %v", ok1, ok2, busy, !busy)
	}
	assertStoredInOrder(t, holder, keys)
	if !same {
		if _, ok := other.Get(keys[0]); ok {
			t.Errorf("Get(a) found in both maps, but a is linked into one")
		}
		assertStoredInOrder(t, other, keys[1:])
	}
}

func Test_D3StoreItemIntoTwoMapsAfterTheCheck(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.set.beforeInit", false, true)
}

func Test_D3StoreItemIntoTwoMapsAfterTheFind(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.add2.found", false, true)
}

func Test_D3StoreItemIntoTwoMapsBeforeTheLink(t *testing.T) {
	testD3StoreItemAtOnce(t, "elist.insert.begin", false, true)
}

func Test_D3StoreItemTwiceIntoOneMapBeforeTheLookup(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.storeItem.checked", true, false)
}

func Test_D3StoreItemTwiceIntoOneMapAfterTheCheck(t *testing.T) {
	testD3StoreItemAtOnce(t, "map.set.beforeInit", true, true)
}

func Test_D3StoreItemTwiceIntoOneMapBeforeTheLink(t *testing.T) {
	testD3StoreItemAtOnce(t, "elist.insert.begin", true, true)
}

// D3: G1 stores a into m1 and stops after it found a not linked, before it
// takes the mapIsBusy of a. G2 stores a into m2, and G3 purges the key of a
// from m2 and stops after it took a out of the list, before it clears the
// links of a. G1 goes on, and then G3. G1 must return false, as G3 holds the
// mapIsBusy of a, and the list of m1 must stay whole. G1 cannot stop after
// it took the flag here: G2 would then return false at once.
func Test_D3StoreItemWhileTheOtherMapPurgesIt(t *testing.T) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[1:])
	setKeys(t, m2, keys[1:])

	s := newStepper(t)
	st1 := s.stopAt("map.storeItem.checked", isNode(nodeOf(a)))
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

	if ok1 {
		t.Errorf("m1.StoreItem(a) returned true while m2.Purge(a) held a")
	}
	assertStoredInOrder(t, m2, keys[1:])
	assertStoredInOrder(t, m1, keys[1:])
}

// D3: G1 stores a into m1 and stops after it found a not linked, before it
// takes the mapIsBusy of a. G2 stores a into m2, and G3 deletes the key of a
// from m2; both return true. When G1 goes on, it must return false, as a is
// linked into m2, and must not undo the Delete of G3: a stays deleted in m2.
// G1 cannot stop after it took the flag here: G2 would then return false at
// once.
func Test_D3StoreItemAfterTheCheckKeepsADeleteOfTheOtherMap(t *testing.T) {
	keys := adjacentKeys(2)
	items := newStepItems(keys[:1])
	a := &items[0]
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[1:])
	setKeys(t, m2, keys[1:])

	s := newStepper(t)
	st := s.stopAt("map.storeItem.checked", isNode(nodeOf(a)))
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
// purges it when purge is set: it finds a, but G1 holds the mapIsBusy of a,
// so it must return false and leave a as it is. G3 purges z and stops after
// it marked z, so that the insert of G1 fails and puts a back. G1 must link
// a again and return true: the map holds x and a.
func testD3StoreItemRefusesADeleteBetweenTheCASesOfItsInsert(t *testing.T, purge bool) {
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
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(a) })
	st1.waitReached(t, done1)
	if purge {
		if m.base.Purge(keys[1]) {
			t.Errorf("Purge(a) returned true while StoreItem(a) held a")
		}
	} else if m.base.Delete(keys[1]) {
		t.Errorf("Delete(a) returned true while StoreItem(a) held a")
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
	waitDone(t, done3, "Purge(z)")

	if !ok1 {
		t.Errorf("StoreItem(a) returned false")
	}
	assertStoredInOrder(t, m, keys[:2])
}

// Keys x < u < z < zz; the map holds x, z and zz. G1 stores u and stops after
// it linked u from x. GP purges the key of u and stops after it found u. GQ
// purges z and stops after it marked z, so that the insert of G1 fails and
// puts u back; GQ then ends, and G1 takes u again for the place before zz and
// stops before it links u. GP goes on: G1 holds the mapIsBusy of u, so GP
// must return false without clearing the links of u. G1 then links u, and
// the list must hold x, u and zz.
func Test_D3StoreItemTakingItsItemAgainRefusesAPurgeThatFoundItBefore(t *testing.T) {
	keys := adjacentKeys(4)
	items := newStepItems(keys[1:2])
	u := &items[0]
	m := newStepMap()
	setKeys(t, m, []string{keys[0], keys[2], keys[3]})
	zItem, ok := m.base.LoadItem(keys[2])
	if !ok {
		t.Fatalf("LoadItem(z) not found")
	}
	z := nodeOf(zItem.(*skiplistmap.SampleItem))

	s := newStepper(t)
	st1 := s.stopAt("elist.add.cas2", isNode(nodeOf(u)))
	var ok1, okP bool
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(u) })
	st1.waitReached(t, done1)
	stP := s.stopAt("map.delete.found", isNode(nodeOf(u)))
	doneP := goStep(t, func() { okP = m.base.Purge(keys[1]) })
	stP.waitReached(t, doneP)
	stQ := s.stopAt("elist.del.marked", isNode(z))
	doneQ := goStep(t, func() { m.base.Purge(keys[2]) })
	stQ.waitReached(t, doneQ)
	again := s.stopAt("elist.add.cas1", isNode(nodeOf(u)))
	st1.Release()
	stQ.Release()
	waitDone(t, doneQ, "Purge(z)")
	again.waitReached(t, done1)
	stP.Release()
	waitDone(t, doneP, "Purge(u)")
	again.Release()
	waitDone(t, done1, "StoreItem(u)")

	if okP || !ok1 {
		t.Errorf("Purge(u) = %v and StoreItem(u) = %v, want false and true", okP, ok1)
	}
	assertStoredInOrder(t, m, []string{keys[0], keys[1], keys[3]})
}

// Items u and w have the same key. G1 stores u into m1 and stops after it
// found u not linked. u is stored into m2, and w into m1. When G1 goes on, it
// takes the mapIsBusy of u and finds u linked into m2: it must return false
// and leave the value of w.
func Test_D3StoreItemOfAnItemLinkedMeanwhileLeavesTheItemOfItsKey(t *testing.T) {
	keys := adjacentKeys(1)
	items := newStepItems([]string{keys[0], keys[0]})
	u, w := &items[0], &items[1]
	m1, m2 := newStepMap(), newStepMap()

	s := newStepper(t)
	st := s.stopAt("map.storeItem.checked", isNode(nodeOf(u)))
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m1.base.StoreItem(u) })
	st.waitReached(t, done1)
	if !m2.base.StoreItem(u) {
		t.Fatalf("m2.StoreItem(u) returned false")
	}
	if !m1.base.StoreItem(w) {
		t.Fatalf("m1.StoreItem(w) returned false")
	}
	st.Release()
	waitDone(t, done1, "m1.StoreItem(u)")

	if ok1 {
		t.Errorf("m1.StoreItem(u) returned true, but u is linked into m2")
	}
	if got, _ := m1.Get(keys[0]); got != w.Value() {
		t.Errorf("m1.Get(k) = %p, want the value of w %p", got, w.Value())
	}
}

// Keys x < u < z lie in the last bucket; the map holds x and z. G1 stores u
// and stops before add2 looks for the position from x; G2 purges x. G1 then
// inserts u after the dummy of the bucket and stops after it linked u from
// the dummy. G3 purges the key of u: G1 holds the mapIsBusy of u, so G3 must
// return false and leave u as it is. G4 purges z and stops after it marked z,
// so that the insert fails. G1 then goes to the insert before the last dummy,
// links u there and returns true: the map holds u.
func Test_D3StoreItemRefusesAPurgeBeforeItsInsertAtTheTail(t *testing.T) {
	keys := regionKeys(0xf, 0xf, 3)
	items := newStepItems(keys[1:2])
	u := &items[0]
	m := newStepMap()
	setKeys(t, m, []string{keys[0], keys[2]})
	xItem, ok := m.base.LoadItem(keys[0])
	if !ok {
		t.Fatalf("LoadItem(x) not found")
	}
	x := nodeOf(xItem.(*skiplistmap.SampleItem))
	zItem, ok := m.base.LoadItem(keys[2])
	if !ok {
		t.Fatalf("LoadItem(z) not found")
	}
	z := nodeOf(zItem.(*skiplistmap.SampleItem))

	s := newStepper(t)
	st1 := s.stopAt("map.find.begin", isNode(x))
	var ok1 bool
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(u) })
	st1.waitReached(t, done1)
	if !m.base.Purge(keys[0]) {
		t.Fatalf("Purge(x) returned false")
	}
	cas2 := s.stopAt("elist.add.cas2", isNode(nodeOf(u)))
	st1.Release()
	cas2.waitReached(t, done1)
	if m.base.Purge(keys[1]) {
		t.Errorf("Purge(u) returned true while StoreItem(u) held u")
	}
	st4 := s.stopAt("elist.del.marked", isNode(z))
	done4 := goStep(t, func() { m.base.Purge(keys[2]) })
	st4.waitReached(t, done4)
	tail := s.stopAt("map.add2.tailInsert", isNode(nodeOf(u)))
	cas2.Release()
	select {
	case <-tail.reached:
	case <-done1:
	}
	st4.Release()
	waitDone(t, done4, "Purge(z)")
	tail.Release()
	waitDone(t, done1, "StoreItem(u)")

	if !ok1 {
		t.Errorf("StoreItem(u) returned false")
	}
	assertStoredInOrder(t, m, keys[1:2])
}

func Test_D3StoreItemRefusesADeleteBetweenTheCASesOfItsInsert(t *testing.T) {
	testD3StoreItemRefusesADeleteBetweenTheCASesOfItsInsert(t, false)
}

func Test_D3StoreItemRefusesAPurgeBetweenTheCASesOfItsInsert(t *testing.T) {
	testD3StoreItemRefusesADeleteBetweenTheCASesOfItsInsert(t, true)
}
