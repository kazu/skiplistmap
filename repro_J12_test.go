//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Before the fix, Set of a key not present in a map with the embedded pool
// locked the muPool of the bucket it had found, and inserted into the pool of
// that bucket. downLevels[0] of a bucket (firstDown) uses the pool of its
// parent, so a Set that found the parent and a Set that found firstDown
// changed one pool under two locks at the same time.
//
// Keys: keys with the top 4 bits 3 of the reversed hash in a map with the
// embedded pool and at most 16 items per bucket. a and b are not stored and
// have reversed hashes in 0x30..0x33; x is the key whose Set splits the top
// bucket P of 0x3.. for the first time. The keys stored before x hold a key
// above a and b and below 0x38.., so that a and b stay in the pool of P after
// the split and are inserted in the middle of it.
//
// Interleaving: G2 sets b. It finds P, which has not split, and stops at
// set.newKeyLock before it locks the muPool that guards the insertion. G1
// sets x to the end: the split makes firstDown, and a lookup of a key of
// 0x30..0x37 now finds firstDown. G2 resumes, locks the muPool it chose
// before the split, and stops at insertToPool.publish, after it linked the
// new array of the pool of P with b into the list of entries and before it
// stores the array into the pool. G3 sets a: it finds firstDown and stops at
// set.newKeyLock, and the test compares the two muPools. Before the fix G3
// locks firstDown.muPool, which G2 does not hold, and inserts a into the pool
// of P while G2 is inside insertToPool of the same pool. When G2 resumes, it
// stores its array, which does not hold a, into the pool: a stays linked in
// the list of entries, but Get, which searches the pool, misses a. After the
// fix both lock the muPool of P, the owner of the pool, and G3 waits for G2.
func Test_ReproJ12NewKeySetsLockTwoMutexesOfOnePool(t *testing.T) {
	var cand []string
	for i := 0; len(cand) < 60; i++ {
		if k := crashKey(i); reverseOf(k)>>60 == 3 {
			cand = append(cand, k)
		}
	}
	var a, b string
	var seq []string
	for _, k := range cand {
		switch r := reverseOf(k) >> 56; {
		case r < 0x34 && a == "":
			a = k
		case r < 0x34 && b == "":
			b = k
		default:
			seq = append(seq, k)
		}
	}
	lowest := uint64(3) << 60

	// Find n: the Set of seq[n] splits P for the first time.
	probe := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)))
	n := -1
	for i, k := range seq {
		probe.Set(k, &list_head.ListHead{})
		if found, base := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](probe.base, lowest); found != base {
			n = i
			break
		}
	}
	if n < 0 {
		t.Fatalf("no Set split the bucket of 0x3..")
	}
	x := seq[n]
	above := false
	for _, k := range seq[:n] {
		r := reverseOf(k)
		if r > reverseOf(a) && r > reverseOf(b) && r < uint64(0x38)<<56 {
			above = true
		}
	}
	if !above {
		t.Fatalf("no key stored before the split lies above a and b in the pool of P")
	}

	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any](skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, any](true), skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](16)))
	for _, k := range seq[:n] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	if found, base := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, lowest); found != base {
		t.Fatalf("the bucket of 0x3.. has split before the test")
	}

	s := newStepper(t)
	lock2 := s.stopAt("map.set.newKeyLock", nil)
	var ok1, ok2, ok3 bool
	done2 := goStep(t, func() { ok2 = m.Set(b, &list_head.ListHead{}) })
	lock2.waitReached(t, done2)
	t.Logf("Set(b) found the bucket %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](lock2.a))

	done1 := goStep(t, func() { ok1 = m.Set(x, &list_head.ListHead{}) })
	waitDone(t, done1, "Set(x)")
	found, base := skiplistmap.StepLockBuckets[skiplistmap.StringKey, any](m.base, reverseOf(a))
	if found == base {
		t.Fatalf("Set(x) did not split the bucket of 0x3..")
	}

	publish := s.stopAt("map.insertToPool.publish", nil)
	lock2.Release()
	publish.waitReached(t, done2)

	lock3 := s.stopAt("map.set.newKeyLock", nil)
	done3 := goStep(t, func() { ok3 = m.Set(a, &list_head.ListHead{}) })
	lock3.waitReached(t, done3)
	t.Logf("Set(a) found the bucket %016x; the lookup of a finds firstDown: %v", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](lock3.a), lock3.a == found)
	t.Logf("Set(a) locks the muPool that Set(b) holds: %v", lock3.b == lock2.b)
	lock3.Release()
	if waitAtMost(done3, 2*time.Second) {
		t.Errorf("Set(a) finished while Set(b) was inserting into the same pool")
	}

	publish.Release()
	waitDone(t, done2, "Set(b)")
	waitDone(t, done3, "Set(a)")
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("Set(x) = %v, Set(b) = %v, Set(a) = %v, want all true", ok1, ok2, ok3)
	}
	for _, k := range []struct{ name, key string }{{"a", a}, {"b", b}} {
		if _, ok := m.Get(k.key); !ok {
			t.Errorf("Get(%s %q) not found: the key is lost", k.name, k.key)
		}
	}
	assertStoredInOrder(t, m, append(append([]string{}, seq[:n+1]...), a, b))
}
