//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// J23 (3.3.2): in skiplistmap4 a broken level list does not lose a key.
//
// What keeps the keys: a level list only chooses the bucket where a walk of
// the list of entries starts, and every walk goes from its start toward the
// key.
//   - Get and LoadItem choose the bucket from the downLevels (findBucket) and
//     the list of buckets (prevAsB in searchBucket4update and
//     searchBucket4Key4). Only CombineSearch2 steps once on the level list
//     (NextOnLevel in _searchBybucket), and _searchBybucket then walks
//     forward from the dummy of that bucket if its reverse is below the key,
//     and backward otherwise.
//   - _set steps on the level list (NextOnLevel) to a bucket whose reverse is
//     not above the key, or takes searchBucket if it finds none, and find
//     walks forward from its dummy to the first entry not below the key.
//   - makeBucket uses the level list (findNextLevelBucket) only for the
//     position of the new bucket in that level list.
// So as long as the list of entries is in order, the start does not change
// what the walk finds.

// j23Run runs goroutines one step at a time, like r332Run, but to their end,
// and at the points in j23Points only, so that every interleaving can be
// played in seconds. A split is stepped after it read the list of buckets
// ("makeBucket.pairFound"), claimed the bucket ("makeBucket.claimed"), walked
// the list of buckets ("insertBucket.begin"), linked the dummy
// ("insertBucket.dummyLinked") and the bucket ("makeBucket.added"), and walked
// the level list ("makeBucket.levelFound"), and inside each insertion into the
// list of buckets and into the level list, at the points of lista where it
// reads and changes links. The walks themselves and the linking of the dummy
// run as one step each: the points inside them (makeBucket.pairWalk,
// find.begin, add2.found and the other points of lista) are not stepped.
// With them the schedules exceed a million; the interleavings of the list of
// buckets at those points are played by Test_Repro_3_3_2_ExploreBucketListSplits.
type j23Run struct {
	mu     sync.Mutex
	owner  map[int64]int
	events chan r332Event
	resume []chan struct{}
	trace  []string
}

var j23Points = map[string]bool{
	"makeBucket.pairFound":     true,
	"makeBucket.claimed":       true,
	"insertBucket.begin":       true,
	"insertBucket.dummyLinked": true,
	"makeBucket.added":         true,
	"makeBucket.levelFound":    true,
	"lista.insert.begin":       true,
	"lista.add.cas1":           true,
	"lista.add.cas2":           true,
	"lista.add.rollback":       true,
}

func (r *j23Run) step(point string) {
	if !j23Points[point] {
		return
	}
	id := r332Gid()
	r.mu.Lock()
	g, ok := r.owner[id]
	r.mu.Unlock()
	if !ok {
		return
	}
	r.events <- r332Event{g: g, point: point}
	<-r.resume[g]
}

// play runs ops following the schedule prefix and then always picks the first
// goroutine that can go on, as r332Run.play does.
func (r *j23Run) play(t *testing.T, ops []func() error, prefix []int) (choices [][2]int, errs []error, ok bool) {
	r.owner = map[int64]int{}
	r.events = make(chan r332Event)
	r.resume = make([]chan struct{}, len(ops))
	errs = make([]error, len(ops))
	for g := range ops {
		r.resume[g] = make(chan struct{})
	}
	SetStepHook(func(p string, _, _ unsafe.Pointer) { r.step(p) })
	list_head.SetStepHook(func(p string, _, _, _ *list_head.ListHead) { r.step("lista." + p) })
	defer SetStepHook(nil)
	defer list_head.SetStepHook(nil)

	for g := range ops {
		go func(g int) {
			r.mu.Lock()
			r.owner[r332Gid()] = g
			r.mu.Unlock()
			r.events <- r332Event{g: g, point: "start"}
			<-r.resume[g]
			err := ops[g]()
			r.mu.Lock()
			delete(r.owner, r332Gid())
			r.mu.Unlock()
			r.events <- r332Event{g: g, done: true, err: err}
		}(g)
	}
	parked := make([]bool, len(ops))
	finished := make([]bool, len(ops))
	at := make([]string, len(ops))
	wait := func() bool {
		select {
		case ev := <-r.events:
			if ev.done {
				finished[ev.g], errs[ev.g] = true, ev.err
				r.trace = append(r.trace, fmt.Sprintf("G%d returns %v", ev.g+1, ev.err))
			} else {
				parked[ev.g], at[ev.g] = true, ev.point
				r.trace = append(r.trace, fmt.Sprintf("G%d at %s", ev.g+1, ev.point))
			}
			return true
		case <-time.After(5 * time.Second):
			t.Errorf("a goroutine did not reach a step point\n%s", strings.Join(r.trace, "\n"))
			return false
		}
	}
	for range ops {
		if !wait() {
			return nil, nil, false
		}
	}
	for {
		var can []int
		for g := range ops {
			if parked[g] && !finished[g] {
				can = append(can, g)
			}
		}
		if len(can) == 0 {
			break
		}
		pick := 0
		if len(choices) < len(prefix) {
			pick = prefix[len(choices)]
		}
		choices = append(choices, [2]int{pick, len(can)})
		g := can[pick]
		// A goroutine at "lista.add.cas1" that comes back to it retries an
		// insertion whose first CAS failed; the failed try changed only the
		// links of its own new node. It runs on until it leaves the retry,
		// by success or by giving up after the last try, as one step.
		for from := at[g]; ; {
			parked[g] = false
			r.resume[g] <- struct{}{}
			if !wait() {
				return nil, nil, false
			}
			if from != "lista.add.cas1" || !parked[g] || at[g] != from {
				break
			}
		}
		if len(choices) > 2000 {
			t.Errorf("schedule does not end\n%s", strings.Join(r.trace, "\n"))
			return nil, nil, false
		}
	}
	return choices, errs, true
}

// j23Keys returns n keys for each top 8 bits of the reversed hash from 0x10
// to 0x1f: the range of h.buckets[1], whose level 2 list the splits break.
func j23Keys(n int) []string {
	count := map[uint64]int{}
	var keys []string
	for i := 0; len(keys) < 16*n; i++ {
		k := fmt.Sprintf("j23-%d", i)
		kh, _ := KeyToHash(k)
		top := bits.Reverse64(kh) >> 56
		if top>>4 != 1 || count[top] == n {
			continue
		}
		count[top]++
		keys = append(keys, k)
	}
	return keys
}

// j23Probe stores more keys into the map after the race, splits some of their
// buckets, and then checks that the list of entries is in order and that
// every key is found by Get in NestedSearchForBucket (the default) and in
// CombineSearch2, and passed by RangeItem once. It returns the first failure.
func j23Probe(h *Map, keys, more []string) error {
	for _, k := range more {
		if !h.Set(k, k) {
			return fmt.Errorf("Set(%q) = false", k)
		}
	}
	for i := 0; i < len(more); i += 5 {
		item, ok := h.LoadItem(more[i])
		if !ok {
			return fmt.Errorf("LoadItem(%q) not found before the splits", more[i])
		}
		h.makeBucket(item.PtrListHead(), 0)
	}
	if err := StepCheckLists(h); err != nil {
		return err
	}
	all := append(append([]string{}, keys...), more...)
	mode := h.modeForBucket
	defer func() { h.modeForBucket = mode }()
	for _, m := range []SearchMode{mode, CombineSearch2} {
		h.modeForBucket = m
		for _, k := range all {
			if v, ok := h.Get(k); !ok || v != k {
				return fmt.Errorf("Get(%q) = %v, %v in search mode %d", k, v, ok, m)
			}
		}
	}
	seen := map[string]int{}
	h.RangeItem(func(item MapItem) bool {
		seen[item.Key().(string)]++
		return true
	})
	for _, k := range all {
		if seen[k] != 1 {
			return fmt.Errorf("RangeItem passed %q %d times", k, seen[k])
		}
	}
	if len(seen) != len(all) {
		return fmt.Errorf("RangeItem passed %d keys, want %d", len(seen), len(all))
	}
	return nil
}

// Evidence for J23, on the race that breaks the level 2 list
// (Test_Repro_3_3_2_MakeBucketLevelInsertIntoSameGap): the map holds two keys
// near 0x11<<56 and has split h.buckets[1] once at 0x18<<56. G1 and G2 both
// split the bucket of the smaller key, so each makes 0x14<<56 or, after the
// other linked 0x14<<56 into the list of buckets, 0x12<<56.
//
// Every interleaving of the two at the points of j23Run, from their start to
// their end, is played: the reads of the lists, the claims, and the
// insertions into the list of buckets and into the level 2 list. After each one
// j23Probe stores 48 more keys over 0x10..0x1f (their _set steps on the level
// 2 list), splits the buckets of some of them (findNextLevelBucket walks the
// level 2 list), and checks the lists and every key. The level 2 list is out
// of order in some of the schedules; the test fails if it is in none, since
// then it shows nothing.
func Test_Repro_J23_BrokenLevelListKeepsKeys(t *testing.T) {
	keys := r332Keys(0x11, 2)
	more := j23Keys(3)
	var prefix []int
	played, broken := 0, 0
	for {
		h, split := r332SplitMap(t)
		split() // returns ErrBucketInvalidOrder after a linking in order (J22)
		var r j23Run
		choices, errs, ok := r.play(t, []func() error{split, split}, prefix)
		if !ok {
			return
		}
		played++
		lr := r332LevelReverses(h, 2)
		levelBroken := false
		for i := 1; i < len(lr); i++ {
			if lr[i-1] <= lr[i] {
				levelBroken = true
			}
		}
		if levelBroken {
			broken++
		}
		if err := j23Probe(h, keys, more); err != nil {
			t.Fatalf("schedule %v (level 2 list %s, broken %v): %v\nerrors %v\n%s", choices, r332Hex(lr), levelBroken, err, errs, strings.Join(r.trace, "\n"))
		}
		if prefix = r332NextPrefix(choices); prefix == nil {
			break
		}
	}
	t.Logf("%d schedules played; the level 2 list was out of order after %d of them, and every key was found after all", played, broken)
	if broken == 0 {
		t.Errorf("no schedule broke the level 2 list; the test shows nothing")
	}
}

// Evidence for J23, on the part that keeps the keys: a walk of the list of
// entries finds the key from the dummy of any bucket. On a map that split its
// buckets by itself, for every bucket b in the list of buckets and every key
// k, _searchBybucket(b, k) finds k in both search modes, and when the reverse
// of b is not above k, find from the dummy of b stops at the same entry as
// find from the head of the list, the position _set links k before.
func Test_Repro_J23_SearchFromAnyBucket(t *testing.T) {
	h := New(MaxPefBucket(4))
	keys := j23Keys(8)
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("j23-any-%d", i))
	}
	for _, k := range keys {
		if !h.Set(k, k) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	if err := StepCheckLists(h); err != nil {
		t.Fatal(err)
	}
	var buckets []*bucket
	for cur := h.headBucket.DirectNext(); cur != h.tailBucket; cur = cur.DirectNext() {
		buckets = append(buckets, bucketFromListHead(cur))
	}
	levels := 0
	for l := int32(2); l <= 16 && !h.isEmptyBylevel(l); l++ {
		levels++
	}
	if len(buckets) <= 16 || levels == 0 {
		t.Fatalf("the map has %d buckets and %d levels below the top; it did not split", len(buckets), levels)
	}
	mode := h.modeForBucket
	defer func() { h.modeForBucket = mode }()
	for _, k := range keys {
		kh, _ := KeyToHash(k)
		rk := bits.Reverse64(kh)
		cond := func(e HMapEntry) bool { return rk <= e.PtrMapHead().reverse }
		want, _ := h.find(h.head.DirectNext(), cond)
		for _, b := range buckets {
			for _, m := range []SearchMode{mode, CombineSearch2} {
				h.modeForBucket = m
				e := h._searchBybucket(b, rk, true)
				if e == nil || e.PtrMapHead().reverse != rk {
					t.Fatalf("_searchBybucket(bucket %#x<<56, %q) = %v in search mode %d", b.reverse>>56, k, e, m)
				}
			}
			if b.reverse > rk {
				continue
			}
			if got, _ := h.find(b.head(), cond); got != want {
				t.Fatalf("find(dummy of bucket %#x<<56) for %q stopped at a different entry than find from the head", b.reverse>>56, k)
			}
		}
	}
	t.Logf("%d keys searched from each of %d buckets (%d levels below the top)", len(keys), len(buckets), levels)
}
