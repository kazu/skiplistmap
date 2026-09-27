//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_UpdateDuringTwoExpandsIsKept replays this order:
//
//  1. Maps m1 and m2 each fill a pool with 64 adjacent keys.
//  2. G1 sets the 65th key of m1: _expand copies the items of its pool and
//     stops at pool.expand.copied.
//  3. G2 sets the 65th key of m2: _expand of the pool of m2 starts and stops
//     at pool.expand.copied too. Two expands run.
//  4. The main goroutine sets k, a key of the pool of m1, to v2. Its item was
//     copied by G1 before this store.
//  5. G2 and then G1 end. Get(k) on m1 must return v2.
func Test_UpdateDuringTwoExpandsIsKept(t *testing.T) {
	holdPoolArrays(t)
	n := skiplistmap.CntOfPersamepleItemPool
	keys := adjacentKeys(n + 1)
	m1, m2 := newStepMap(), newStepMap()
	setKeys(t, m1, keys[:n])
	setKeys(t, m2, keys[:n])
	k := keys[5]
	v2 := &list_head.ListHead{}

	s := newStepper(t)
	copied1 := s.stopAt("map.pool.expand.copied", nil)
	done1 := goStep(t, func() { m1.Set(keys[n], &list_head.ListHead{}) })
	copied1.waitReached(t, done1)
	copied2 := s.stopAt("map.pool.expand.copied", nil)
	done2 := goStep(t, func() { m2.Set(keys[n], &list_head.ListHead{}) })
	copied2.waitReached(t, done2)

	done3 := goStep(t, func() { m1.Set(k, v2) })
	waitAtMost(done3, 200*time.Millisecond)
	copied2.Release()
	waitDone(t, done2, "Set of the 65th key of m2")
	copied1.Release()
	waitDone(t, done1, "Set of the 65th key of m1")
	waitDone(t, done3, "Set(k, v2)")

	if v, ok := m1.Get(k); !ok || v != v2 {
		t.Errorf("Get(k) = %p, %v, want v2 %p", v, ok, v2)
	}
}
