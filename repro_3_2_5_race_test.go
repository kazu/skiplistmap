//go:build stephook && race

package skiplistmap_test

import (
	"testing"

	"github.com/kazu/skiplistmap"
)

// The two splits of b in Test_Repro_3_2_5_SplitGetsClaimedDownLevel, stopped
// where both hold b2 and then let run on together with the hooks removed.
// G1 stops at makeBucket2.recurse holding the muPool of T. G2, holding the
// muPool of b, claims element 12 of the downLevels of T as b2 and stops at
// makeBucket2.got. G1 then recurses into makeBucket2(b), gets the same b2
// through the path for an element already made, and stops at makeBucket2.got
// too. Once released, both run Init on b2 and on its LevelHead and split the
// pool of b, and nothing orders the two. This fails only through the race
// detector.
func Test_Repro_3_2_5_RaceSplitsOfSameBucket(t *testing.T) {
	p := newR325Map(t)
	s, stop1, stop2, done1, done2 := splitTwice(t, p, "map.makeBucket2.got")
	b2 := stop2.a
	if r := skiplistmap.StepBucketReverse(b2); r != p.split {
		t.Fatalf("G2 got a bucket of reverse %016x, want %016x", r, p.split)
	}

	stop3 := s.stopAt("map.makeBucket2.got", isNode(b2))
	stop1.Release()
	stop3.waitReached(t, done1)

	runOn(t, stop3, stop2, false)
	waitDone(t, done1, "G1 (Set of the low key)")
	waitDone(t, done2, "G2 (Set of u4)")
}
