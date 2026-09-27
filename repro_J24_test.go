//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// j24Map is a map with the embedded pool (skiplistmap5) in which Purge(x)
// and Set(b) delete adjacent nodes of the entry list at the same time
// through the map API:
//   - x, s and z are stored keys, adjacent in the list in this order, in the
//     item pool of one bucket (base), and a lookup of each of them finds
//     downLevels[0] of that bucket (found), not base;
//   - s has been deleted with Delete, so its item stays linked, marked
//     deleted;
//   - b is a key not stored, between s and z, in the same pool.
//
// Purge(x) locks found.muPool, and Set(b) of a new key locks base.muPool
// (3.2.1), so the two do not wait for each other. Set(b) reuses the slot of
// s (getWithFn, foundFree) and runs MarkForDelete on it while Purge(x) runs
// MarkForDelete on x, the node before it.
//
// q is a stored key in another bucket. Purge sets the traversal mode shared
// by the whole process to WaitNoMark while it deletes and puts back what it
// saw (3.2.3); a Purge(q) that starts before Purge(x) and ends after it began
// puts back Direct while Purge(x) is still running, so that the Set(b) below
// walks the list in the mode it has when no Purge runs.
type j24Map struct {
	m             *WrapHMap
	stored        []string // the keys the map must hold at the end
	x, s, z, b, q string
	p, nx, ns, nz unsafe.Pointer // p is the node before x
}

func newJ24Map(t *testing.T) *j24Map {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	var keys, rest []string
	q := ""
	for i := 0; len(keys)+len(rest) < 1000 || q == ""; i++ {
		k := crashKey(i)
		switch top := reverseOf(k) >> 60; {
		case top == 5 && q == "":
			q = k
		case top != 3:
		case len(keys) < 40:
			keys = append(keys, k)
		default:
			rest = append(rest, k)
		}
	}
	for _, k := range append(append([]string{}, keys...), q) {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })
	node := func(k string) unsafe.Pointer {
		it, ok := m.base.LoadItem(k)
		if !ok {
			t.Fatalf("LoadItem(%q) not found", k)
		}
		return unsafe.Pointer(it.PtrListHead())
	}
	for i := 0; i+2 < len(keys); i++ {
		x, s, z := keys[i], keys[i+1], keys[i+2]
		nx, ns, nz := node(x), node(s), node(z)
		if m24Next(nx) != ns || m24Next(ns) != nz {
			continue
		}
		found, base := skiplistmap.StepM24LockBuckets(m.base, reverseOf(x))
		if found == base {
			continue
		}
		if f, b := skiplistmap.StepM24LockBuckets(m.base, reverseOf(s)); f != found || b != base {
			continue
		}
		if f, b := skiplistmap.StepM24LockBuckets(m.base, reverseOf(z)); f != found || b != base {
			continue
		}
		for _, b := range rest {
			if reverseOf(s) < reverseOf(b) && reverseOf(b) < reverseOf(z) {
				if _, bb := skiplistmap.StepM24LockBuckets(m.base, reverseOf(b)); bb == base {
					j := &j24Map{m: m, x: x, s: s, z: z, b: b, q: q, p: m24Prev(nx), nx: nx, ns: ns, nz: nz}
					for _, k := range keys {
						if k != x && k != s {
							j.stored = append(j.stored, k)
						}
					}
					j.stored = append(j.stored, b)
					if !m.base.Delete(s) {
						t.Fatalf("Delete(%q) = false", s)
					}
					t.Logf("p=%016x x=%016x (%q) s=%016x (%q) b=%016x (%q) z=%016x (%q)", skiplistmap.StepListReverse(j.p),
						reverseOf(x), x, reverseOf(s), s, reverseOf(b), b, reverseOf(z), z)
					return j
				}
			}
		}
	}
	t.Fatalf("no adjacent keys x, s, z whose lookups find downLevels[0] of the bucket that owns their pool")
	return nil
}

// start stops Purge(q) at elist.del.marked of q, then runs Purge(x) and
// stops it at xPoint of x, and lets Purge(q) finish, which puts back Direct.
func (j *j24Map) start(t *testing.T, s *stepper, xPoint string) (x *stepStop, xDone <-chan struct{}) {
	t.Helper()
	qNode := func() unsafe.Pointer {
		it, _ := j.m.base.LoadItem(j.q)
		return unsafe.Pointer(it.PtrListHead())
	}()
	q := s.stopAt("elist.del.marked", isNode(qNode))
	qDone := goStep(t, func() { j.m.base.Purge(j.q) })
	q.waitReached(t, qDone)
	x = s.stopAt(xPoint, isNode(j.nx))
	xDone = goStep(t, func() { j.m.base.Purge(j.x) })
	x.waitReached(t, xDone)
	q.Release()
	waitDone(t, qDone, "Purge(q)")
	return x, xDone
}

// finish releases the stopped goroutines, waits for them, and checks the
// map: x, s and q are gone, b and the other keys are stored.
func (j *j24Map) finish(t *testing.T, stops []*stepStop, dones ...<-chan struct{}) {
	t.Helper()
	for _, st := range stops {
		st.Release()
	}
	for _, d := range dones {
		if !m24Finished(d, 10*time.Second) {
			t.Errorf("a stopped goroutine did not finish")
			return
		}
	}
	m24CheckStored(t, j.m, j.stored)
	runtime.KeepAlive(j)
}

// J24 (3.4.1): MarkListHead writes the next that MarkForDelete read, not the
// next the link has now.
//
// Interleaving:
//  1. Purge(x) reads p and s as the prev and the next of x and stops at
//     elist.del.begin, before it marks the links of x.
//  2. Set(b) takes the slot of s (it is deleted), and its MarkForDelete of s
//     moves x.next from s to z and z.prev from s to x. _set runs Init on s
//     and add2 stops at map.find.begin, before it links s (now b) again.
//  3. Purge(x) resumes and stops at elist.del.check, after it changed the
//     links to x.
//
// Before the fix of 3.4.1 (elist 671d1c4) the mark writes s back into x.next,
// and MarkForDelete then moves p.next from x to s, which Init unlinked: the
// forward chain from p ends at s, and z and the nodes after it are cut off.
func Test_ReproJ24PurgeMarksStaleNextOfReusedSlot(t *testing.T) {
	j := newJ24Map(t)
	s := newStepper(t)
	x, xDone := j.start(t, s, "elist.del.begin")

	sInit := s.stopAt("map.set.beforeInit", isNode(j.ns))
	bDone := goStep(t, func() { j.m.Set(j.b, &list_head.ListHead{}) })
	sInit.waitReached(t, bDone)
	bFind := s.stopAt("map.find.begin", nil)
	sInit.Release()
	bFind.waitReached(t, bDone)
	t.Logf("Set(b) unlinked s and ran Init on it: x.next is z: %v; s is self-linked: %v", m24Next(j.nx) == j.nz, m24SelfLinked(j.ns))

	xCheck := s.stopAt("elist.del.check", isNode(j.nx))
	x.Release()
	xCheck.waitReached(t, xDone)
	t.Logf("Purge(x) changed the links to x: p.next=%p (s=%p z=%p); x.next=%p", m24Next(j.p), j.ns, j.nz, m24Next(j.nx))
	if m24Next(j.p) == j.ns && m24SelfLinked(j.ns) {
		t.Errorf("p.next points to s, which Init unlinked; the forward chain from p ends at s and cuts off z")
	}
	j.finish(t, []*stepStop{xCheck, bFind}, xDone, bDone)
}

// J24 (3.4.4): NextNoM passes over a live node after a marked one.
//
// Interleaving:
//  1. Purge(x) marks both links of x and stops at elist.del.marked.
//  2. Set(b) takes the slot of s, marks both links of s and stops at
//     elist.del.marked.
//  3. Purge(x) resumes: it finds the next of x that is not marked, and moves
//     p.next from x to it. It stops at elist.del.check.
//
// p.next must be z: s is marked, z is not. Before the fix of 3.4.4 (elist
// 2abbcd4) NextNoM goes on from z to the node after it, and p.next skips z,
// which is live.
func Test_ReproJ24PurgePassesOverLiveNode(t *testing.T) {
	j := newJ24Map(t)
	s := newStepper(t)
	x, xDone := j.start(t, s, "elist.del.marked")

	sMarked := s.stopAt("elist.del.marked", isNode(j.ns))
	bDone := goStep(t, func() { j.m.Set(j.b, &list_head.ListHead{}) })
	sMarked.waitReached(t, bDone)

	xCheck := s.stopAt("elist.del.check", isNode(j.nx))
	x.Release()
	xCheck.waitReached(t, xDone)
	t.Logf("Purge(x) changed the links to x: p.next=%p (s=%p z=%p, the node after z=%p)", m24Next(j.p), j.ns, j.nz, m24Next(j.nz))
	if n := m24Next(j.p); n != j.nz && n != j.ns {
		t.Errorf("p.next skips z, which is live: it points to %016x", skiplistmap.StepListReverse(n))
	}
	j.finish(t, []*stepStop{xCheck, sMarked}, xDone, bDone)
}

// J24 (3.4.5): MarkForDelete returns while the marked next of a node being
// deleted before it still points to the node.
//
// Interleaving:
//  1. Purge(x) marks both links of x and stops at elist.del.marked.
//  2. Set(b) takes the slot of s, marks both links of s and stops at
//     elist.del.marked.
//  3. Set(b) resumes: MarkForDelete of s moves z.prev from s to p. x.next is
//     marked, so it is not moved, and MarkForDelete returns. _set runs Init
//     on s and add2 stops at map.find.begin, before it links s (now b) again.
//  4. Purge(x) resumes and stops at elist.del.check, after it changed the
//     links to x.
//
// p.next must not point to s, which Init unlinked. Before a fix it does: the
// next of x that MarkForDelete read is s, which is no longer marked after
// Init, so Purge(x) moves p.next from x to s, and the forward chain from p
// ends at s and cuts off z.
func Test_ReproJ24PurgeLinksToNeighborInitedBySet(t *testing.T) {
	j := newJ24Map(t)
	s := newStepper(t)
	x, xDone := j.start(t, s, "elist.del.marked")

	sMarked := s.stopAt("elist.del.marked", isNode(j.ns))
	bDone := goStep(t, func() { j.m.Set(j.b, &list_head.ListHead{}) })
	sMarked.waitReached(t, bDone)
	sInit := s.stopAt("map.set.beforeInit", isNode(j.ns))
	sMarked.Release()
	if !m24Reached(t, sInit, bDone) {
		t.Fatalf("Set(b) finished without reaching map.set.beforeInit of s")
	}
	bFind := s.stopAt("map.find.begin", nil)
	sInit.Release()
	bFind.waitReached(t, bDone)
	t.Logf("Set(b) unlinked s and ran Init on it: z.prev is p: %v; x.next still points to s: %v; s is self-linked: %v",
		m24Prev(j.nz) == j.p, m24Next(j.nx) == j.ns, m24SelfLinked(j.ns))

	xCheck := s.stopAt("elist.del.check", isNode(j.nx))
	x.Release()
	xCheck.waitReached(t, xDone)
	t.Logf("Purge(x) changed the links to x: p.next=%p (s=%p z=%p)", m24Next(j.p), j.ns, j.nz)
	if m24Next(j.p) == j.ns && m24SelfLinked(j.ns) {
		t.Errorf("p.next points to s, which Init unlinked; the forward chain from p ends at s and cuts off z")
	}
	j.finish(t, []*stepStop{xCheck, bFind}, xDone, bDone)
}

// m24GoRecover runs fn in a new goroutine and returns a channel closed when
// fn returns, and the value fn panicked with, which is read after done is
// closed. Unlike goStep it does not fail the test on a panic.
func m24GoRecover(fn func()) (done <-chan struct{}, panicked *any) {
	d := make(chan struct{})
	var p any
	go func() {
		defer close(d)
		defer func() { p = recover() }()
		fn()
	}()
	return d, &p
}

// J24 (3.4.1, 3.4.4 and 3.4.6 of lista, through pool._expand): the lista
// defects need a neighbor of the deleted pool that another goroutine changes
// (3.4.1), deletes (3.4.4) or still has pointing to the pool (3.4.6) while
// _expand deletes it. In the map none of that happens:
//   - every write to the links of a pool list is in _expand (MarkForDelete of
//     the pool, InsertBefore of the new pool, Init of the pool), under the
//     mutex of the pool it replaces;
//   - a pool list holds one pool: _expand links the new pool only after it
//     unlinked the old one, and Get takes a next pool only when one is
//     linked, which is never;
//   - a second _expand of the same pool (3.1.3) runs after the first one
//     released the mutex, when Init has linked the pool to a start and an
//     end of its own (lista Init in concurrent mode), so its MarkForDelete
//     writes only those, and it panics at RepaireSliceAfterCopy before it
//     links anything into the pool list.
//
// The orders below are the ones two writers of one pool list can take (the
// second one waits for the mutex, or runs after the first one ended); the
// test checks at each MarkForDelete and at the IsSafety of _expand that the
// neighbors of the pool are the head and the end of the pool list, which no
// goroutine deletes, or its own start and end, and that no node of the pool
// list points to the pool when IsSafety is asked.
//
// Keys: 66 keys that share one pool of a map without the embedded pool. The
// main goroutine sets the first 64, which fill the pool sp.
//  1. G_c sets the 65th key, finds sp full and no next pool, and stops at
//     map.pool.expand.begin, before it locks sp.mu.
//  2. G_b sets the 66th key and runs _expand of sp. It stops at
//     map.pool.expand.marked (sp unlinked) and at
//     map.pool.expand.beforeSafety (the new pool linked), and finishes.
//  3. G_c resumes, locks sp.mu and runs _expand of sp again; it stops at
//     map.pool.expand.marked, and then panics with "already deleted" (3.1.3,
//     not checked here).
func Test_ReproJ24ExpandDeletesPoolBetweenSentinels(t *testing.T) {
	t.Cleanup(func() { list_head.DefaultModeTraverse.Option(list_head.Direct()) })
	keys := adjacentKeys(skiplistmap.CntOfPersamepleItemPool + 2)
	m := newStepMap()
	for _, k := range keys[:skiplistmap.CntOfPersamepleItemPool] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	r := reverseOf(keys[0])
	head, tail, pools := skiplistmap.StepM24PoolList(m.base, r)
	if len(pools) != 1 {
		t.Fatalf("the pool list holds %d pools, want 1", len(pools))
	}
	sp := pools[0]
	spNode, _, _ := skiplistmap.StepM24PoolLinks(sp)
	checkList := func(when string, want unsafe.Pointer) {
		t.Helper()
		_, _, got := skiplistmap.StepM24PoolList(m.base, r)
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s: the pool list holds %p, want only %p", when, got, want)
		}
		wantNode, prev, next := skiplistmap.StepM24PoolLinks(want)
		hPrev, hNext := skiplistmap.StepM24ListaLinks(head)
		tPrev, _ := skiplistmap.StepM24ListaLinks(tail)
		if prev != head || next != tail || hNext != wantNode || tPrev != wantNode {
			t.Errorf("%s: head.next=%p pool.prev=%p pool.next=%p tail.prev=%p, want head %p <-> %p <-> tail %p", when, hNext, prev, next, tPrev, head, wantNode, tail)
		}
		if hNext == spNode || tPrev == spNode || hPrev == spNode {
			t.Errorf("%s: a node of the pool list points to sp", when)
		}
	}

	s := newStepper(t)
	cBegin := s.stopAt("map.pool.expand.begin", isNode(sp))
	cDone, cPanic := m24GoRecover(func() { m.Set(keys[64], &list_head.ListHead{}) })
	cBegin.waitReached(t, cDone)

	bMarked := s.stopAt("map.pool.expand.marked", isNode(sp))
	bDone := goStep(t, func() { m.Set(keys[65], &list_head.ListHead{}) })
	bMarked.waitReached(t, bDone)
	_, prev, next := skiplistmap.StepM24PoolLinks(sp)
	t.Logf("G_b marked sp: sp.prev is the head: %v, sp.next is the tail: %v", prev == head, next == tail)
	if prev != head || next != tail {
		t.Errorf("G_b deletes sp between %p and %p, want the head %p and the tail %p of the pool list", prev, next, head, tail)
	}
	if _, hNext := skiplistmap.StepM24ListaLinks(head); hNext != tail {
		t.Errorf("after G_b unlinked sp, head.next is %p, want the tail %p", hNext, tail)
	}

	bSafety := s.stopAt("map.pool.expand.beforeSafety", isNode(sp))
	bMarked.Release()
	bSafety.waitReached(t, bDone)
	nPool := bSafety.b
	checkList("before G_b asks IsSafety of sp", nPool)
	bSafety.Release()
	waitDone(t, bDone, "Set by G_b")
	_, prev, next = skiplistmap.StepM24PoolLinks(sp)
	t.Logf("G_b ran Init on sp: sp.prev is the head: %v, sp.next is the tail: %v", prev == head, next == tail)
	checkList("after G_b", nPool)

	cMarked := s.stopAt("map.pool.expand.marked", isNode(sp))
	cBegin.Release()
	if m24Reached(t, cMarked, cDone) {
		_, prev, next = skiplistmap.StepM24PoolLinks(sp)
		t.Logf("G_c marked sp again: sp.prev is the head: %v, sp.next is the tail: %v", prev == head, next == tail)
		if prev == head || next == tail {
			t.Errorf("G_c deletes sp between %p and %p, a node of the pool list", prev, next)
		}
		checkList("when G_c marked sp again", nPool)
		cMarked.Release()
	}
	if !m24Finished(cDone, 10*time.Second) {
		t.Fatalf("Set by G_c did not finish")
	}
	t.Logf("G_c ended with panic %v (3.1.3)", *cPanic)
	checkList("after G_c", nPool)
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Errorf("%v", err)
	}
}
