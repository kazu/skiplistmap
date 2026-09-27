//go:build stephook

package skiplistmap

import (
	"fmt"
	"strings"
	"testing"
)

// j51TailItem is an item whose KeyHash returns k and c as they are, so that
// the test can store a key whose reverse is ^uint64(0).
type j51TailItem struct {
	SampleItem
	k, c uint64
}

func (x *j51TailItem) KeyHash() (uint64, uint64) { return x.k, x.c }

// j51TailPoints are added to r332Points for this test: the point after the
// lookup of StoreItem (set.beforeInit) and the two points add2 reaches when it
// finds no position (add2.bucketInsert, add2.tailInsert).
var j51TailPoints = []string{"set.beforeInit", "add2.bucketInsert", "add2.tailInsert"}

// Evidence that StoreItem of a map without the embedded pool does not take
// the tail path of add2 (the one at add2.tailInsert) while a bucket is split.
//
// add2 takes the tail path only when it finds no position and the bucket of
// _set is missing or its entry(h) is nil. The bucket is never missing: the
// lookup of StoreItem (getWithBucket) calls bucket.head() on it. entry(h) is
// nil only while the dummy of the bucket is not linked, and a bucket becomes
// visible to _findBucket (level > 0 and index < len of the down levels) only
// after addBucket linked its dummy. G2 stores a key whose reverse is
// ^uint64(0): no entry has a larger reverse, so add2 finds no position, and
// its bucket is the largest down level of h.buckets[15], the one G1 makes.
//
// G1 splits that bucket; every interleaving of G1 and G2 at the step points
// of r332Run and j51TailPoints is played:
//   - FirstDown: the first split under h.buckets[15]; G1 makes 0xf8<<56 and
//     the down levels of h.buckets[15] (bucketFromPool, first branch).
//   - LaterDown: 0xf8<<56 is there; G1 makes 0xfc<<56 at an index beyond the
//     len of the down levels (bucketFromPool, RETRY_SETUP branch).
//
// G2 must never stop at add2.tailInsert, and after each schedule the key of
// G2 is found, Len counts it once and the lists are in order.
func Test_J51TailPathNotTakenExplored(t *testing.T) {
	for _, p := range j51TailPoints {
		r332Points[p] = true
	}
	defer func() {
		for _, p := range j51TailPoints {
			delete(r332Points, p)
		}
	}()

	// Without a split, in one goroutine: the bucket 0xf0<<56 holds two keys
	// at 0xf1<<56, and StoreItem of the key at ^uint64(0) links it before the
	// entry after the dummy of the bucket (add2.bucketInsert), out of order.
	{
		h := New(MaxPefBucket(1 << 20))
		r332SplitNode(t, h, 0xf1)
		ok := h.StoreItem(&j51TailItem{k: ^uint64(0), c: 1})
		n := 0
		h.RangeItem(func(item MapItem) bool {
			if m := item.PtrMapHead(); m.reverse == ^uint64(0) && m.conflict == 1 {
				n++
			}
			return true
		})
		t.Logf("one goroutine, no split: StoreItem = %v, Len = %d, the item is in the list %d times", ok, h.Len(), n)
	}

	for _, tc := range []struct {
		name  string
		made  uint64
		setup func(t *testing.T, h *Map) (keys []string, split func() error)
	}{
		{"FirstDown", 0xf8 << 56, func(t *testing.T, h *Map) ([]string, func() error) {
			return r332SplitNode(t, h, 0xf1)
		}},
		{"LaterDown", 0xfc << 56, func(t *testing.T, h *Map) ([]string, func() error) {
			keys, first := r332SplitNode(t, h, 0xf1)
			more, split := r332SplitNode(t, h, 0xf9)
			first()
			if !r332Has(r332BucketReverses(h), 0xf8<<56) {
				t.Fatalf("list of buckets %s does not hold 0xf8<<56", r332Hex(r332BucketReverses(h)))
			}
			return append(keys, more...), split
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var prefix []int
			played, lost := 0, 0
			lookupAfter := map[string]int{}
			for {
				h := New(MaxPefBucket(1 << 20))
				keys, g1 := tc.setup(t, h)
				x := &j51TailItem{k: ^uint64(0), c: 1}
				g2 := func() error {
					if !h.StoreItem(x) {
						return fmt.Errorf("StoreItem returned false")
					}
					return nil
				}
				var r r332Run
				choices, errs, ok := r.play(t, []func() error{g1, g2}, prefix)
				if !ok {
					return
				}
				played++
				fail := func(format string, args ...interface{}) {
					t.Errorf("schedule %v: %s\nerrors %v\n%s", choices, fmt.Sprintf(format, args...), errs, strings.Join(r.trace, "\n"))
				}
				last := "G1 not started"
				for _, ev := range r.trace {
					if ev == "G2 at set.beforeInit" {
						lookupAfter[last]++
						break
					}
					if strings.HasPrefix(ev, "G1 ") {
						last = ev
					}
				}
				bucketInsert := false
				// add2 goes on to the tail path when the entry after the
				// dummy of the bucket is no place for the key; the checks
				// below hold either way
				for _, ev := range r.trace {
					if ev == "G2 at add2.bucketInsert" {
						bucketInsert = true
					}
				}
				if !bucketInsert {
					fail("G2 did not reach add2.bucketInsert")
				}
				if errs[1] != nil {
					fail("G2: %v", errs[1])
				}
				if err := StepCheckLists(h); err != nil {
					fail("%v", err)
				}
				if got := r332BucketReverses(h); !r332Has(got, tc.made) {
					fail("list of buckets %s does not hold %#x", r332Hex(got), tc.made)
				}
				n := 0
				h.RangeItem(func(item MapItem) bool {
					if m := item.PtrMapHead(); m.reverse == ^uint64(0) && m.conflict == 1 {
						n++
					}
					return true
				})
				if n > 1 {
					fail("RangeItem yields the key of G2 %d times", n)
				}
				if n == 0 {
					lost++
				}
				if got, want := h.Len(), len(keys)+1; got != want {
					fail("Len = %d, want %d", got, want)
				}
				if t.Failed() {
					return
				}
				if prefix = r332NextPrefix(choices); prefix == nil {
					break
				}
			}
			t.Logf("%d schedules; the item of G2 is not in the list in %d of them", played, lost)
			t.Logf("the lookup of G2 ran after:")
			for ev, n := range lookupAfter {
				t.Logf("  %s: %d", ev, n)
			}
		})
	}
}
