//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// J32 (3.4.12): the rollback CAS of an insertion can fail, and when the
// insertion then retries with the same node and succeeds, in the scene of
// 3.1.2 a node that another insertion linked in front of it is left linked
// only from the old array of a pool, and is lost.
//
// Keys x < a1 < ... < a64 < m < n share the top 4 bits of the reversed hash,
// so they are adjacent in the list of a map that does not split buckets. The
// main goroutine sets a1..a64, which fill the 64 items of one pool in the
// order of the keys; the node after a64 (the old a64, called p) is o, which
// lies outside the pool. M and N are items that do not come from the pool,
// with the keys m and n.
//
// Interleaving:
//  1. M: StoreItem(M) chooses p as the node to find its position from and
//     stops at map.set.beforeInit.
//  2. N: StoreItem(N) finds o as its position from p, reads p as the prev of
//     o, and stops at elist.add.cas1, before its first CAS.
//  3. The main goroutine sets x: the pool is full, so _expand copies its
//     items into a new array and RepaireSliceAfterCopy moves the links from
//     outside into the copy: o.prev from p to the new a64 (p'). Set(x)
//     finishes. The old p still points to o.
//  4. N resumes: its first CAS moves p.next from o to N on the old array.
//     Its second CAS expects o.prev to be p, finds p', and fails. N stops at
//     elist.add.rollback.
//  5. M resumes: it runs Init on M, finds N as its position from p, reads p
//     as the prev of N, and links M: p.next from N to M, N.prev from p to M.
//     StoreItem(M) returns true.
//  6. N resumes: the rollback CAS expects p.next to be N, finds M, and fails.
//     The rollback puts the links of N back to what they were before the
//     insertion (self-linked, from the Init of _set).
//
// Up to 53f4580 insertBefore retries with the same node N: the prev of o is
// now p', so both CASes succeed on the new array, p' -> N -> o, and
// StoreItem(N) returns true. M stays linked only from the old p, which the
// list no longer reaches: M is lost. From a7b359c on add2 retries from its
// find instead, starting at p; the test checks the map in the same way.
func Test_ReproJ32RollbackFailsAndRetryLosesFrontNode(t *testing.T) {
	m24HoldGC(t)
	keys := adjacentKeys(skiplistmap.CntOfPersamepleItemPool + 3)
	kx := keys[0]
	pool := keys[1 : 1+skiplistmap.CntOfPersamepleItemPool]
	km, kn := keys[len(keys)-2], keys[len(keys)-1]
	ext := newStepItems([]string{km, kn})
	mNode, nNode := nodeOf(&ext[0]), nodeOf(&ext[1])
	mp := newStepMap()
	for _, k := range pool {
		if !mp.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	last, ok := mp.base.LoadItem(pool[len(pool)-1])
	if !ok {
		t.Fatalf("LoadItem(%q) failed", pool[len(pool)-1])
	}
	p := unsafe.Pointer(last.PtrListHead())
	o := m24Next(p)
	t.Logf("x=%016x a64=%016x m=%016x n=%016x o=%016x",
		reverseOf(kx), reverseOf(pool[len(pool)-1]), reverseOf(km), reverseOf(kn), skiplistmap.StepListReverse(o))

	s := newStepper(t)
	mBefore := s.stopAt("map.set.beforeInit", isNode(mNode))
	mDone := goStep(t, func() {
		if !mp.base.StoreItem(&ext[0]) {
			t.Errorf("StoreItem(M) returned false")
		}
	})
	mBefore.waitReached(t, mDone)
	if mBefore.b != p {
		mBefore.Release()
		t.Fatalf("StoreItem(M) chose %p as the start, want p %p", mBefore.b, p)
	}

	nCas1 := s.stopAt("elist.add.cas1", isNode(nNode))
	nDone := goStep(t, func() {
		if !mp.base.StoreItem(&ext[1]) {
			t.Errorf("StoreItem(N) returned false")
		}
	})
	nCas1.waitReached(t, nDone)
	if nCas1.b != p || m24Next(nNode) != o {
		nCas1.Release()
		mBefore.Release()
		t.Fatalf("StoreItem(N) links N between %p and %p, want p %p and o %p", nCas1.b, m24Next(nNode), p, o)
	}

	if !mp.Set(kx, &list_head.ListHead{}) {
		t.Fatalf("Set(%q) failed", kx)
	}
	pNew := m24Prev(o)
	t.Logf("_expand finished: o.prev moved from p to %p: %v; p.next is still o: %v", pNew, pNew != p, m24Next(p) == o)

	nRollback := s.stopAt("elist.add.rollback", isNode(nNode))
	nCas1.Release()
	if !m24Reached(t, nRollback, nDone) {
		mBefore.Release()
		t.Fatalf("the second CAS of N did not fail: p.next=%p o.prev=%p (p=%p N=%p o=%p)", m24Next(p), m24Prev(o), p, nNode, o)
	}

	mBefore.Release()
	waitDone(t, mDone, "StoreItem(M)")
	t.Logf("M linked in front of N: p.next is M: %v; M.next is N: %v; N.prev is M: %v",
		m24Next(p) == mNode, m24Next(mNode) == nNode, m24Prev(nNode) == mNode)

	nRollback.Release()
	if !m24Finished(nDone, 10*time.Second) {
		t.Fatalf("StoreItem(N) did not finish")
	}
	t.Logf("N returned: p'.next is N: %v, N.next is o: %v, N is self-linked: %v; p.next is M: %v, M.next is N: %v",
		m24Next(pNew) == nNode, m24Next(nNode) == o, m24SelfLinked(nNode), m24Next(p) == mNode, m24Next(mNode) == nNode)
	if m24Next(pNew) == nNode && m24Next(nNode) == o && m24Next(p) == mNode {
		t.Errorf("N is linked after p' on the new array, and M is linked only from the old p")
	}

	m24CheckStored(t, mp, append(append([]string{kx}, pool...), km, kn))
	runtime.KeepAlive(ext)
}
