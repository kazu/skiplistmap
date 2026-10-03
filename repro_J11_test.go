//go:build stephook

package skiplistmap_test

import (
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// j11Keys is a choice of keys for the tests of J11.
type j11Keys struct {
	first []string // stored before Purge(a) starts
	extra []string // stored after first; the last one makes the bucket of a split
	a     string   // a key of first
	next  string   // the stored key after a
	b     string   // a key not stored, between a and next
}

func newJ11Map(t *testing.T) *WrapHMap {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })
	return newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)))
}

func j11Set(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	for _, k := range keys {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
}

// chooseJ11Keys picks the keys on a map that runs the Sets of the tests
// without the interleaving. The hash seed differs between processes, so it
// tries the keys of each value of the top 4 bits of the reversed hash in turn
// until tryJ11Keys finds a choice.
func chooseJ11Keys(t *testing.T) j11Keys {
	t.Helper()
	for top := uint64(1); top < 15; top++ {
		var all []string
		for i := 0; len(all) < 1000; i++ {
			if k := crashKey(i); reverseOf(k)>>60 == top {
				all = append(all, k)
			}
		}
		if k, ok := tryJ11Keys(t, all[:10], all[10:]); ok {
			return k
		}
	}
	t.Fatalf("no split moved a stored key, still in its slot, to a bucket that owns its pool with a free key after it")
	return j11Keys{}
}

// tryJ11Keys stores first, 10 keys that share the top 4 bits of the reversed
// hash, so that their bucket does not split, and notes the bucket that a
// lookup of each key finds. It then stores the keys of rest above all of
// first, in the order of their reversed hashes, so that each is appended to
// the pool, one by one until a split moves a stored key a of first to a
// bucket that owns its own pool: a lookup of a now finds a bucket that is the
// owner of its pool and is not the bucket found before, and a is still in
// its slot. next is the stored key after a and b a key not stored between a
// and next, and the lookups of both find the same owner as a.
func tryJ11Keys(t *testing.T, first, rest []string) (j11Keys, bool) {
	t.Helper()
	var maxFirst uint64
	for _, k := range first {
		if r := reverseOf(k); r > maxFirst {
			maxFirst = r
		}
	}
	var ext []string
	for _, k := range rest {
		if reverseOf(k) > maxFirst {
			ext = append(ext, k)
		}
	}
	sort.Slice(ext, func(x, y int) bool { return reverseOf(ext[x]) < reverseOf(ext[y]) })

	m := newJ11Map(t)
	j11Set(t, m, first)
	before := map[string]unsafe.Pointer{}
	nodes := map[string]unsafe.Pointer{}
	for _, k := range first {
		before[k], _ = skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(k))
		nodes[k] = j11Node(t, m, k)
	}
	for j := 0; j+1 < len(ext); j++ {
		j11Set(t, m, ext[j:j+1])
		stored := append(append([]string{}, first...), ext[:j+1]...)
		sort.Slice(stored, func(x, y int) bool { return reverseOf(stored[x]) < reverseOf(stored[y]) })
		for i := 0; i+1 < len(stored); i++ {
			a, next := stored[i], stored[i+1]
			foundA, ok := before[a]
			if !ok || j11Node(t, m, a) != nodes[a] {
				continue
			}
			fa, ba := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(a))
			if fa != ba || ba == foundA {
				continue
			}
			if _, bn := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(next)); bn != ba {
				continue
			}
			for _, b := range rest {
				if reverseOf(a) < reverseOf(b) && reverseOf(b) < reverseOf(next) {
					if _, bb := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(b)); bb == ba {
						return j11Keys{first: first, extra: ext[:j+1], a: a, next: next, b: b}, true
					}
				}
			}
		}
	}
	return j11Keys{}, false
}

// j11Node returns the list node of the item that holds key in m.
func j11Node(t *testing.T, m *WrapHMap, key string) unsafe.Pointer {
	t.Helper()
	item, ok := m.base.LoadItemForTest(skiplistmap.StringKey(key))
	if !ok {
		t.Fatalf("LoadItem(%q) not found", key)
	}
	return unsafe.Pointer(item.PtrListHead())
}

// expect returns the keys stored by first and extra, without a and with b.
func (k j11Keys) expect() []string {
	var keys []string
	for _, x := range append(append([]string{}, k.first...), k.extra...) {
		if x != k.a {
			keys = append(keys, x)
		}
	}
	return append(keys, k.b)
}

// purgeWithSplitBeforeLock runs the schedule of the tests below and reports
// whether Set(b) finished while Purge(a) was stopped. With splitWhileFound,
// the keys of extra are stored while Purge(a) is stopped between its lookup
// and its lock; without it, they are stored before Purge(a) starts.
func purgeWithSplitBeforeLock(t *testing.T, splitWhileFound bool, wait time.Duration) (setFinished bool) {
	t.Helper()
	k := chooseJ11Keys(t)
	m := newJ11Map(t)
	j11Set(t, m, k.first)
	if !splitWhileFound {
		j11Set(t, m, k.extra)
	}
	itemA, ok := m.base.LoadItemForTest(skiplistmap.StringKey(k.a))
	if !ok {
		t.Fatalf("LoadItem(%q) not found", k.a)
	}
	nodeA := unsafe.Pointer(itemA.PtrListHead())

	s := newStepper(t)
	found := s.stopAt("map.loadItem.found", isNode(nodeA))
	del := s.stopAt("elist.del.begin", isNode(nodeA))
	var ok1, ok2 bool
	done1 := goStep(t, func() { ok1 = m.Delete(k.a) })
	found.waitReached(t, done1)
	if splitWhileFound {
		j11Set(t, m, k.extra)
		if j11Node(t, m, k.a) != nodeA {
			t.Fatalf("the Sets of %q moved a to another slot", k.extra)
		}
	}
	// found.b is the bucket whose muPool loadItem locks next.
	_, ownerB := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(k.b))
	if (ownerB != found.b) != splitWhileFound {
		t.Fatalf("Purge(%q) locks %p, Set(%q) locks %p; want them different only with the split while found (%v)",
			k.a, found.b, k.b, ownerB, splitWhileFound)
	}
	found.Release()
	del.waitReached(t, done1)

	done2 := goStep(t, func() { ok2 = m.Set(k.b, &list_head.ListHead{}) })
	setFinished = waitAtMost(done2, wait)
	if setFinished {
		if itemB, ok := m.base.LoadItemForTest(skiplistmap.StringKey(k.b)); ok {
			t.Logf("Set(%q) finished while Purge(%q) was stopped; b reused the slot of a: %v",
				k.b, k.a, unsafe.Pointer(itemB.PtrListHead()) == nodeA)
		}
	}

	del.Release()
	waitDone(t, done1, "Purge")
	waitDone(t, done2, "Set")
	t.Logf("MarkForDelete on the slot of a, by Purge(a) or by Set(b) reusing it, read its links %d times and marked them %d times",
		s.count("elist.del.begin", nodeA), s.count("elist.del.marked", nodeA))

	if !ok1 || !ok2 {
		t.Fatalf("Purge(%q) = %v, Set(%q) = %v, want both true", k.a, ok1, k.b, ok2)
	}
	if _, ok := m.Get(k.a); ok {
		t.Errorf("Get(%q) found the purged key", k.a)
	}
	if got, want := m.base.Len(), len(k.expect()); got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
	assertStoredInOrder(t, m, k.expect())
	return setFinished
}

// Keys a < b < next share the top 4 bits of the reversed hash; a and next are
// stored, b is not. The bucket of a has not split yet.
//
// G1 purges a. loadItem finds the item of a in the bucket B and stops before
// it tries the lock of B. The main goroutine then stores more keys of B until
// B splits: the range of a, b and next moves to a new bucket B', which owns
// its own pool, so a Set of a key there locks B'.muPool. G1 resumes: nothing
// holds B.muPool, so its TryLock succeeds, and it goes on with the bucket it
// found before the split. Purge marks a deleted and stops at the start of
// MarkForDelete, after it read the links of a (prev and next) but before it
// marks them. G2 then sets b. It locks B'.muPool, which G1 does not hold, so
// it does not wait: it finds the slot of a free, unlinks it, reuses it for b
// and links b between prev and next. When G1 resumes, its MarkForDelete
// succeeds on the slot, which now holds b and still points to next, and
// unlinks b.
//
// The keys stored while G1 is stopped all have reversed hashes above the
// stored ones, so each is appended to the pool and a stays in the slot that
// G1 found: the only change G1 misses is the split.
//
// Both Purge(a) and Set(b) return true, yet the list of entries does not
// hold b.
func Test_Repro_J11_SplitBetweenLookupAndLock(t *testing.T) {
	if purgeWithSplitBeforeLock(t, true, 5*time.Second) {
		t.Logf("Set finished while Purge held the muPool of the bucket it found before the split")
	}
}

// The schedule of Test_Repro_J11_SplitBetweenLookupAndLock with the split
// before Purge(a) starts, so that loadItem finds B' and Purge(a) and Set(b)
// both lock B'.muPool. G2 waits for G1 and runs after Purge(a) returns, and
// the map holds b. The loss above comes from the split between the lookup
// and the lock.
func Test_Repro_J11_SplitBeforeLookup(t *testing.T) {
	if purgeWithSplitBeforeLock(t, false, time.Second) {
		t.Errorf("Set finished while Purge held the muPool of the owner")
	}
}
