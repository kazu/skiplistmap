//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// r325Map is a map with the embedded pool and at most 3 items per bucket.
// The top bucket 3 (T) holds the keys u[0..3], whose second digits of the
// reversed hash are 8, 9, d and e. Setting them split T once at 0x38, which
// moved nothing, so every key is still in the pool of T.
type r325Map struct {
	m     *WrapHMap
	u     []string // u[0..3] are stored; u[4], whose second digit is f, is not
	low   string   // a key of T whose second digit is 0; not stored
	split uint64   // the reverse of the bucket that the second split makes
}

func newR325Map(t *testing.T) *r325Map {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	p := &r325Map{m: newEmbeddedMap(3), split: 0x3c << 56}
	for _, second := range []uint64{0x8, 0x9, 0xd, 0xe, 0xf} {
		p.u = append(p.u, regionKeys(0x3, second, 1)[0])
	}
	p.low = regionKeys(0x3, 0x0, 1)[0]
	for _, k := range p.u[:4] {
		if !p.m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	return p
}

// splitTwice stops the goroutine G1 that sets p.low at makeBucket2.recurse:
// G1 holds the muPool of T, has split T at 0x38 into T = [low] and the new
// bucket b = [u0..u3], and has published b, which is over the limit. It then
// stops the goroutine G2 that sets u4 at second: G2 found b, took the muPool
// of b (not the one G1 holds), stored u4 into the pool of b and, splitting b
// at 0x3c, which takes element 12 of the downLevels of T as the new bucket,
// stopped at second. It returns the stepper, the two stops and the done
// channels. The stops are not released.
func splitTwice(t *testing.T, p *r325Map, second string) (s *stepper, stop1, stop2 *stepStop, done1, done2 <-chan struct{}) {
	t.Helper()
	s = newStepper(t)
	stop1 = s.stopAt("map.makeBucket2.recurse", nil)
	done1 = goStep(t, func() { p.m.Set(p.low, &list_head.ListHead{}) })
	stop1.waitReached(t, done1)
	if r := skiplistmap.StepBucketReverse(stop1.b); r != 0x38<<56 {
		t.Fatalf("G1 split T into a bucket of reverse %016x, want %016x", r, uint64(0x38<<56))
	}

	stop2 = s.stopAt(second, nil)
	done2 = goStep(t, func() { p.m.Set(p.u[4], &list_head.ListHead{}) })
	stop2.waitReached(t, done2)
	if r := skiplistmap.StepBucketReverse(stop2.a); second == "map.bucketFromPoolEmbedded.claimed" && r != p.split {
		t.Fatalf("G2 claimed a bucket of reverse %016x, want %016x", r, p.split)
	}
	return
}

// releaseInOrder lets G1 run to its end, and then G2. A split that waits for
// the muPool of b would wait for G2, so G2 is let go as well if G1 does not
// end in time.
func releaseInOrder(t *testing.T, stop1, stop2 *stepStop, done1, done2 <-chan struct{}) {
	t.Helper()
	stop1.Release()
	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		stop2.Release()
	}
	waitDone(t, done1, "G1 (Set of the low key)")
	stop2.Release()
	waitDone(t, done2, "G2 (Set of u4)")
}

// finishR325 checks what the two splits left. Only one split may take the
// element of downLevels that G2 stopped on. The lists and the buckets are
// checked before any lookup, because a lookup of a key in a bucket with no
// item pool overflows the stack and ends the process.
func finishR325(t *testing.T, p *r325Map, s *stepper, down unsafe.Pointer) {
	t.Helper()
	if n := s.count("map.makeBucket2.got", down); n != 1 {
		t.Errorf("%d splits got the bucket of reverse %016x from bucketFromPoolEmbedded, want 1", n, p.split)
	}
	errLists := skiplistmap.StepCheckLists(p.m.base)
	if errLists != nil {
		t.Errorf("%v", errLists)
	}
	errBuckets := skiplistmap.StepCheckBuckets(p.m.base)
	if errBuckets != nil {
		t.Errorf("%v", errBuckets)
	}
	if errLists != nil || errBuckets != nil {
		return
	}
	assertStoredInOrder(t, p.m, append([]string{p.low}, p.u...))
}

// G1 holds the muPool of T and G2 the muPool of b, so nothing orders the two
// splits of b. G2 stops at bucketFromPoolEmbedded.claim: it has seen that
// element 12 of the downLevels of T has level 0 and has not set its level
// yet. G1 then recurses into makeBucket2(b) at 0x3c, sees level 0 on the same
// element, takes it as b2, splits b into b = [u0, u1] and b2 = [u2, u3, u4],
// links b2 into the list of buckets and returns. G2 then takes the same b2:
// it runs Init on b2, which is linked in the list of buckets, finds nothing
// to split in b (whose length G1 cut to 2), and sets the item pool of b2 to
// nil. The list of buckets stops at b2, and b2 has no item pool, so a lookup
// of u2, u3 or u4 overflows the stack.
func Test_Repro_3_2_5_SplitsClaimSameDownLevel(t *testing.T) {
	p := newR325Map(t)
	s, stop1, stop2, done1, done2 := splitTwice(t, p, "map.bucketFromPoolEmbedded.claim")
	down := stop2.a
	releaseInOrder(t, stop1, stop2, done1, done2)

	if n := s.count("map.bucketFromPoolEmbedded.claim", down); n != 1 {
		t.Errorf("element 12 of the downLevels of T was seen with level 0 by %d splits, want 1", n)
	}
	finishR325(t, p, s, down)
}

// The same two splits, but G2 stops at bucketFromPoolEmbedded.claimed, after
// it has set the level of element 12 to -2. G1 then sees a level that is not
// 0, and bucketFromPoolEmbedded returns the same element to it through the
// path for an element already made. So a CAS on the level alone does not
// keep the two splits apart: the loser still gets the element.
func Test_Repro_3_2_5_SplitGetsClaimedDownLevel(t *testing.T) {
	p := newR325Map(t)
	s, stop1, stop2, done1, done2 := splitTwice(t, p, "map.bucketFromPoolEmbedded.claimed")
	down := stop2.a
	releaseInOrder(t, stop1, stop2, done1, done2)

	finishR325(t, p, s, down)
}
