//go:build stephook

package skiplistmap_test

import (
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// sharedPoolMap is a map with the embedded pool whose bucket of the keys a
// has been split once, so that downLevels[0] of the bucket (firstDown) uses
// the item pool of the bucket itself.
type sharedPoolMap struct {
	m      *WrapHMap
	stored []string // the keys set, in the order of their reversed hashes
	a      string   // a stored key whose lookup finds firstDown
	next   string   // the stored key after a in the pool of a
	b      string   // a key not stored, between a and next, in the pool of a
}

// newSharedPoolMap sets keys that share the top 4 bits of the reversed hash
// into a map with at most 16 items per bucket, then picks a, next and b. With
// split, it sets 40 keys, so that the bucket splits, and a is a key whose
// lookup finds firstDown. Without split, it sets 10 keys, so that the bucket
// does not split, and every lookup finds the bucket that owns the pool.
//
// It puts back the traversal mode of lista, which Purge and RangeItem can
// leave at WaitNoMark for the whole process (3.2.3).
func newSharedPoolMap(t *testing.T, split bool) *sharedPoolMap {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	n := 10
	if split {
		n = 40
	}
	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	var keys, rest []string
	for i := 0; len(keys)+len(rest) < 1000; i++ {
		k := crashKey(i)
		if reverseOf(k)>>60 != 3 {
			continue
		}
		if len(keys) < n {
			keys = append(keys, k)
		} else {
			rest = append(rest, k)
		}
	}
	for _, k := range keys {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })

	for i := 0; i+1 < len(keys); i++ {
		a, next := keys[i], keys[i+1]
		found, base := skiplistmap.StepLockBuckets(m.base, reverseOf(a))
		if (found != base) != split {
			continue
		}
		if _, nbase := skiplistmap.StepLockBuckets(m.base, reverseOf(next)); nbase != base {
			continue
		}
		for _, b := range rest {
			if reverseOf(a) < reverseOf(b) && reverseOf(b) < reverseOf(next) {
				if _, bbase := skiplistmap.StepLockBuckets(m.base, reverseOf(b)); bbase == base {
					return &sharedPoolMap{m: m, stored: keys, a: a, next: next, b: b}
				}
			}
		}
	}
	t.Fatalf("no stored key (split %v) with a free key after it in the same pool", split)
	return nil
}

// expectKeys returns the stored keys without drop and with add.
func (p *sharedPoolMap) expectKeys(drop, add string) []string {
	var keys []string
	for _, k := range p.stored {
		if k != drop {
			keys = append(keys, k)
		}
	}
	if add != "" {
		keys = append(keys, add)
	}
	return keys
}


// purgeWhileSetReusesSlot runs the schedule of the tests below and reports
// whether Set(b) finished while Purge(a) was stopped. It waits up to wait
// for Set(b) before it lets Purge(a) go on.
func purgeWhileSetReusesSlot(t *testing.T, p *sharedPoolMap, wait time.Duration) (setFinished bool) {
	t.Helper()
	m := p.m
	itemA, ok := m.base.LoadItem(p.a)
	if !ok {
		t.Fatalf("LoadItem(%q) not found", p.a)
	}
	nodeA := unsafe.Pointer(itemA.PtrListHead())

	s := newStepper(t)
	st := s.stopAt("elist.del.begin", isNode(nodeA))
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.Delete(p.a) })
	st.waitReached(t, done1)

	done2 := goStep(t, func() { ok2 = m.Set(p.b, &list_head.ListHead{}) })
	setFinished = waitAtMost(done2, wait)

	st.Release()
	waitDone(t, done1, "Purge")
	waitDone(t, done2, "Set")

	if !ok1 || !ok2 {
		t.Fatalf("Purge(%q) = %v, Set(%q) = %v, want both true", p.a, ok1, p.b, ok2)
	}
	if _, ok := m.Get(p.a); ok {
		t.Errorf("Get(%q) found the purged key", p.a)
	}
	assertStoredInOrder(t, m, p.expectKeys(p.a, p.b))
	return setFinished
}

// Keys a < b < next. a and next are stored in the pool of a bucket, and a
// lookup of a finds downLevels[0] of that bucket (firstDown), which uses the
// pool of its parent. b is not stored and belongs to the same pool.
//
// G1 purges a. Purge locks firstDown.muPool, marks a deleted and stops at
// the start of MarkForDelete, after it read the links of a (prev and next)
// but before it marks them. G2 then sets b. Set of a new key locks the
// muPool of the parent, which G1 does not hold, so G2 does not wait: it
// finds the slot of a free, unlinks it, reuses it for b and links b between
// prev and next. When G1 resumes, its MarkForDelete succeeds on the slot,
// which now holds b and still points to next, and unlinks b.
//
// Both Purge(a) and Set(b) return true, yet the list of entries does not
// hold b: RangeItem misses b and Len counts it.
func Test_Repro_3_2_1_PurgeAndNewKeySetShareSlot(t *testing.T) {
	p := newSharedPoolMap(t, true)
	if purgeWhileSetReusesSlot(t, p, 5*time.Second) {
		t.Logf("Set(%q) finished while Purge(%q) held firstDown.muPool", p.b, p.a)
	}
}

// The schedule of Test_Repro_3_2_1_PurgeAndNewKeySetShareSlot on a bucket
// that has not split, so that Purge(a) and Set(b) both lock the muPool of the
// bucket that owns the pool. G2 waits for G1 and runs after Purge(a)
// returns, and the map holds b. The loss above comes from the two locks.
func Test_Repro_3_2_1_PurgeAndNewKeySetSameLock(t *testing.T) {
	p := newSharedPoolMap(t, false)
	if purgeWhileSetReusesSlot(t, p, time.Second) {
		t.Errorf("Set(%q) finished while Purge(%q) held the muPool of the owner", p.b, p.a)
	}
}

// Keys a < b < next as in Test_Repro_3_2_1_PurgeAndNewKeySetShareSlot; a
// lookup of a finds firstDown, and a holds the value v1.
//
// G1 sets a to v2. Set finds the item of a, locks firstDown.muPool and stops
// before it stores v2 into the item. G2 then sets b. Set of a new key locks
// the muPool of the parent, which G1 does not hold, so G2 does not wait: it
// has no free slot to reuse, so insertToPool copies the items of the pool
// into a new array with b among them, v1 in the copy of a. When G1 resumes,
// it stores v2 into the item of a in the old array.
//
// Set(a, v2) returns true, yet Get(a) returns v1: the update is lost.
func Test_Repro_3_2_1_UpdateLostToPoolRebuild(t *testing.T) {
	p := newSharedPoolMap(t, true)
	m := p.m
	v1, ok := m.Get(p.a)
	if !ok {
		t.Fatalf("Get(%q) not found", p.a)
	}
	itemA, _ := m.base.LoadItem(p.a)
	nodeA := unsafe.Pointer(itemA.PtrListHead())

	s := newStepper(t)
	st := s.stopAt("map.set.updateLocked", isNode(nodeA))
	v2 := &list_head.ListHead{}
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.Set(p.a, v2) })
	st.waitReached(t, done1)

	done2 := goStep(t, func() { ok2 = m.Set(p.b, &list_head.ListHead{}) })
	if waitAtMost(done2, 5*time.Second) {
		if item, ok := m.base.LoadItem(p.a); ok && unsafe.Pointer(item.PtrListHead()) != nodeA {
			t.Logf("Set(%q) moved the item of %q while Set(%q) held firstDown.muPool", p.b, p.a, p.a)
		}
	}

	st.Release()
	waitDone(t, done1, "Set(a)")
	waitDone(t, done2, "Set(b)")

	if !ok1 || !ok2 {
		t.Fatalf("Set(%q) = %v, Set(%q) = %v, want both true", p.a, ok1, p.b, ok2)
	}
	switch v, ok := m.Get(p.a); {
	case !ok:
		t.Errorf("Get(%q) not found", p.a)
	case v == v1:
		t.Errorf("Get(%q) returned the old value: Set(%q, v2) was lost", p.a, p.a)
	case v != v2:
		t.Errorf("Get(%q) returned %p, want v2 %p", p.a, v, v2)
	}
	assertStoredInOrder(t, m, p.expectKeys("", p.b))
}

// Keys a < b < next in a bucket that has not split, so that every operation
// on them locks the same muPool.
//
// G2 purges a. loadItem finds the item of a and stops before it tries the
// lock. G1 purges a to the end: it marks the slot deleted, unlinks it and
// returns true. G3 then sets b, reuses the free slot of a for b and links b
// between the item before a and next. When G2 resumes, it locks the muPool,
// which is free, and goes on with the item it found before the lock, which
// now holds b: it marks b deleted, unlinks b and returns true.
//
// Only a was purged, yet the map loses b: Get(b) misses, RangeItem misses b
// and Len drops by two. Locking the owner of the pool does not stop this;
// loadItem has to look the key up again under the lock.
func Test_Repro_3_2_1_PurgeUsesItemFoundBeforeLock(t *testing.T) {
	p := newSharedPoolMap(t, false)
	m := p.m
	itemA, ok := m.base.LoadItem(p.a)
	if !ok {
		t.Fatalf("LoadItem(%q) not found", p.a)
	}
	nodeA := unsafe.Pointer(itemA.PtrListHead())

	s := newStepper(t)
	st := s.stopAt("map.loadItem.found", isNode(nodeA))
	var ok1, ok2, ok3 bool
	done2 := goStep(t, func() { ok2 = m.Delete(p.a) })
	st.waitReached(t, done2)

	done1 := goStep(t, func() { ok1 = m.Delete(p.a) })
	waitDone(t, done1, "the first Purge(a)")
	done3 := goStep(t, func() { ok3 = m.Set(p.b, &list_head.ListHead{}) })
	waitDone(t, done3, "Set(b)")
	if item, ok := m.base.LoadItem(p.b); ok && unsafe.Pointer(item.PtrListHead()) == nodeA {
		t.Logf("Set(%q) reused the slot of %q", p.b, p.a)
	}

	st.Release()
	waitDone(t, done2, "the second Purge(a)")

	if !ok1 || !ok3 {
		t.Fatalf("the first Purge(%q) = %v, Set(%q) = %v, want both true", p.a, ok1, p.b, ok3)
	}
	if ok2 {
		t.Errorf("the second Purge(%q) returned true for a key already purged", p.a)
	}
	if _, ok := m.Get(p.a); ok {
		t.Errorf("Get(%q) found the purged key", p.a)
	}
	assertStoredInOrder(t, m, p.expectKeys(p.a, p.b))
}
