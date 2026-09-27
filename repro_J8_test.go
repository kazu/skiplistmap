//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"

	"github.com/kazu/skiplistmap"
)

// Before the fix, find took the search option "skip bucket dummies" from a
// variable shared by the whole process: it set the option to false, saved
// the value it replaced and put that value back on return. Two finds that
// overlap save and put back each other's values, so a find can walk with the
// option true, skip the dummies and return a position past the next bucket.
// add2 then fails the order check at that position and drops the item.
//
// Keys: k2 has the top 4 bits 3 of the reversed hash, k3 the top 4 bits 4 and
// k1 the top 4 bits 9. The map does not split buckets, so the list of entries
// is ... dummy(3) dummy(4) k3 ... dummy(9) ..., and the process-wide option
// is true, its initial value.
//
// Interleaving: G1 stores k1 and stops at find.begin of its first find, after
// that find set the option to false and saved true. G2 stores k2: its lookup
// runs to the end, and _set chooses dummy(3) as the start of add2
// (set.beforeInit). G2 stops at find.begin of the find of add2, after that
// find set the option to false and saved false, the value G1 set. G1 runs to
// the end: its first find returns and puts back true. G2 resumes with the
// option true: find skips dummy(3) and dummy(4) and returns k3 as the
// position of k2, and the order check of add2 rejects k2 before k3 because the
// entry before k3, dummy(4), is above k2. add2 drops k2 and StoreItem
// returns true. After the fix find takes no option and returns dummy(4).
func Test_ReproJ8FindSkipsDummyByOptionOfOtherFind(t *testing.T) {
	k2 := regionKeys(0x3, 0x0, 1)[0]
	k3 := regionKeys(0x4, 0x0, 1)[0]
	k1 := regionKeys(0x9, 0x0, 1)[0]
	keys := []string{k1, k2, k3}
	items := newStepItems(keys)
	m := newStepMap()
	if !m.base.StoreItem(&items[2]) {
		t.Fatalf("StoreItem(%q) failed", k3)
	}

	s := newStepper(t)
	g1Find := s.stopAt("map.find.begin", nil)
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.base.StoreItem(&items[0]) })
	g1Find.waitReached(t, done1)

	g2Start := s.stopAt("map.set.beforeInit", isNode(nodeOf(&items[1])))
	done2 := goStep(t, func() { ok2 = m.base.StoreItem(&items[1]) })
	g2Start.waitReached(t, done2)
	start := g2Start.b
	t.Logf("add2 of k2 finds its position from %016x", skiplistmap.StepEntryReverse(start))
	g2Find := s.stopAt("map.find.begin", isNode(start))
	g2Start.Release()
	g2Find.waitReached(t, done2)

	g1Find.Release()
	waitDone(t, done1, "StoreItem(k1)")
	g2Find.Release()
	waitDone(t, done2, "StoreItem(k2)")

	if !ok1 || !ok2 {
		t.Fatalf("StoreItem(%q) = %v, StoreItem(%q) = %v, want both true", k1, ok1, k2, ok2)
	}
	if n := s.count("map.add2.found", nodeOf(&items[1])); n > 0 {
		_, pos := s.args("map.add2.found")
		t.Logf("add2 found %016x as the position of k2 %016x (k3 %016x); add2.found of k2 %d times",
			skiplistmap.StepEntryReverse(pos), reverseOf(k2), reverseOf(k3), n)
	}
	if _, ok := m.Get(k2); !ok {
		t.Errorf("Get(%q) not found: add2 dropped k2", k2)
	}
	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}
