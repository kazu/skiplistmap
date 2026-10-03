//go:build stephook

package skiplistmap_test

import (
	"testing"
	"unsafe"

	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_StepJ64LevelListCapInsertsOutOfOrder replays, in one goroutine, the
// split that findNextLevelBucket answers from its limit of 1000 buckets.
// makeBucket now links a bucket with insertOnLevel, and findNextLevelBucket is
// removed; the steps below are those of the code before.
//
// A level list is kept in descending order of reverse. makeBucket asks
// findNextLevelBucket for the first bucket on the level list of the new
// bucket b whose reverse is below b.reverse, and inserts b before it (or after
// it when its reverse is above b.reverse). findNextLevelBucket counts the
// buckets it walks and, at the 1001st, returns it before comparing its
// reverse with b.reverse.
//
//  1. Set stores keys until a level list holds more than 1002 buckets and
//     makeBucket creates a bucket b whose place on that list lies after the
//     1002nd bucket.
//  2. findNextLevelBucket walks 1000 buckets with reverse above b.reverse and
//     returns the 1001st bucket p, whose reverse is also above b.reverse
//     (makeBucket.levelFound reports p). The 1002nd bucket is above
//     b.reverse too.
//  3. makeBucket sees p.reverse > b.reverse and inserts b right after p.
//
// The bucket after b on the level list then has a reverse above b.reverse:
// the level list is out of order.
func Test_StepJ64LevelListCapInsertsOutOfOrder(t *testing.T) {
	h := skiplistmap.NewHMap[skiplistmap.StringKey, any]()

	var (
		found  bool
		level  int32
		bRev   uint64
		pRev   uint64
		pIdx   int
		listed int
	)
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		if found || point != "makeBucket.levelFound" || b == nil {
			return
		}
		l := skiplistmap.StepM27BucketLevel(a)
		_, pr := skiplistmap.StepM27LevelOf(b)
		br := skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](a)
		if pr <= br {
			return
		}
		// p is above b: the walk of the level list returned without comparing.
		rs := skiplistmap.StepM27LevelReverses(h, l)
		for i, r := range rs {
			if r != pr {
				continue
			}
			// The hash seed differs per process, so the place of b
			// differs per run. Wait for a split where the bucket after p
			// is also above b, so that b does not belong right after p.
			if i+1 < len(rs) && rs[i+1] > br {
				found, level, bRev, pRev, pIdx, listed = true, l, br, pr, i, len(rs)
			}
			return
		}
		t.Errorf("level %d: makeBucket.levelFound reported %016x, which is not on the level list", l, pr)
	})
	defer skiplistmap.SetStepHook(nil)

	for i := 0; i < 1<<20 && !found; i++ {
		h.Set(skiplistmap.StringKey(crashKey(i)), &list_head.ListHead{})
	}
	skiplistmap.SetStepHook(nil)
	if !found {
		// no split found its place after a bucket above the one it links;
		// every level list must then be in descending order
		for l := int32(1); l <= 16; l++ {
			rs := skiplistmap.StepM27LevelReverses(h, l)
			for i := 1; i < len(rs); i++ {
				if rs[i] >= rs[i-1] {
					t.Errorf("level %d list out of order: bucket %d (%016x) is not below bucket %d (%016x)",
						l, i+1, rs[i], i, rs[i-1])
					break
				}
			}
		}
		return
	}
	t.Logf("level %d: makeBucket.levelFound reported bucket %d of %d (%016x) for bucket %016x",
		level, pIdx+1, listed, pRev, bRev)
	if pIdx != 1000 {
		t.Errorf("the bucket above the new bucket is bucket %d of the level list, not the 1001st", pIdx+1)
	}

	rs := skiplistmap.StepM27LevelReverses(h, level)
	at := -1
	for i, r := range rs {
		if r == bRev {
			at = i
			break
		}
	}
	if at < 0 || at+1 >= len(rs) {
		t.Fatalf("level %d: bucket %016x is at %d of %d buckets", level, bRev, at, len(rs))
	}
	if rs[at+1] > bRev {
		t.Errorf("level %d list out of order: bucket %d (%016x) is above bucket %d (%016x) before it",
			level, at+2, rs[at+1], at+1, bRev)
	}
}
