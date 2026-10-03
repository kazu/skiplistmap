//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/lista_encabezado"
)

// The pool of the top bucket 3 holds k0 < k1 < k2 < k3. G1 sets k4, which is
// larger than every key in the pool: appendLast raises the length of the pool
// from 4 to 5 and stops at appendLast.claimed. Slot 4 is past the old length,
// so its reverse is still 0. G2 then gets k3. sort.Search in bsearchBybucket
// runs over the 5 slots with "reverse >= r(k3)", which is false at slot 4
// although it is true at slot 3: it reads slot 2 (false), then slot 4 (false),
// and returns 5. bsearchBybucket finds no slot at index 5 and Get(k3) returns
// false, although k3 was stored before the Get started and was never deleted.
func Test_J58AppendLastZeroSlotHidesLastKey(t *testing.T) {
	m, keys := n2MapJ58(t)

	s := newStepper(t)
	stop1 := s.stopAt("map.appendLast.claimed", nil)
	done1 := goStep(t, func() { m.Set(keys[4], &list_head.ListHead{}) })
	stop1.waitReached(t, done1)

	var found bool
	done2 := goStep(t, func() { _, found = m.Get(keys[3]) })
	waitDone(t, done2, "G2")
	stop1.Release()
	waitDone(t, done1, "G1")

	if !found {
		t.Errorf("Get(k3) = false while Set(k4) held slot 4 of the pool with reverse 0; k3 was stored before and never deleted")
	}
	assertStoredInOrder(t, m, keys)
}

// n2MapJ58 returns a map with the embedded pool that does not split its
// buckets, holding k0 < k1 < k2 < k3 of the top bucket 3, and the 5 keys.
func n2MapJ58(t *testing.T) (*WrapHMap, []string) {
	t.Helper()
	m := newEmbeddedMap(32)
	keys := regionKeys(0x3, 0x8, 5)
	for _, k := range keys[:4] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	return m, keys
}
