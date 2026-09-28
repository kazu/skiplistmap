//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// skiplistmap4: u, an item of StoreItem, lies in the list just before the
// item z of the key after it, which is in the array of the pool. G1 purges u
// and stops between its two relinks: the dummy x before u leads to z, and z
// still leads back to u. G2 sets the 65th key, and the pool moves its full
// array. When the move took u as the node before z and ended before G1 went
// on, it led only u to the copy of z: x kept leading to z in the old array,
// and G1 relinked the copy of z back to x and returned true. The move now
// waits for G1, which gives way and tries again while the move relinks u to
// the copies; G1 then must not lead x past the copies either.
func Test_D1PurgeAcrossAnExpandLeavesTheOldArrayLinked(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 2)
	m := newStepMap()
	items := newStepItems(keys[:1])
	u := &items[0]
	if !m.base.StoreItem(u) {
		t.Fatalf("StoreItem(u) failed")
	}
	setKeys(t, m, keys[1:n+1])

	s := newStepper(t)
	st := s.stopAt("elist.del.prevChecked", isNode(nodeOf(u)))
	var purged bool
	done1 := goStep(t, func() { purged = m.base.Purge(keys[0]) })
	st.waitReached(t, done1)
	done2 := goStep(t, func() { m.Set(keys[n+1], &list_head.ListHead{}) })
	waitAtMost(done2, 200*time.Millisecond)
	st.Release()
	waitDone(t, done1, "Purge(u)")
	waitDone(t, done2, "Set of the 65th key")

	if !purged {
		t.Errorf("Purge(u) returned false")
	}
	if _, ok := m.Get(keys[0]); ok {
		t.Errorf("Get(u) found after Purge(u)")
	}
	assertStoredInOrder(t, m, keys[1:])
}
