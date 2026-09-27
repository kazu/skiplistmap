//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The level lists and the list of buckets are kept in descending order of
// reverse: the level 1 list is built so by initBeforeSet, addBucket inserts a
// bucket before the first smaller one, and findNextLevelBucket returns the
// first smaller one. Every insertion walks the list once and then inserts at
// the position it found; the insertion does not check the order again.

// r332LevelReverses returns the reverses of the buckets in the level list of
// level, walked forward from its head.
func r332LevelReverses(h *Map, level int32) []uint64 {
	var got []uint64
	if h.isEmptyBylevel(level) {
		return nil
	}
	head := h.levelBucket(level)
	for cur := head.LevelHead.DirectPrev().DirectNext(); !cur.Empty(); {
		b := bucketFromLevelHead(cur)
		got = append(got, b.reverse)
		cur = b.LevelHead.DirectNext()
		if len(got) > 1000 {
			break
		}
	}
	return got
}

func r332Hex(rs []uint64) string {
	s := "["
	for i, r := range rs {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%#x", r>>56)
	}
	return s + "]<<56"
}

// r332AssertLevel checks that the level list of level holds exactly want.
func r332AssertLevel(t *testing.T, h *Map, level int32, want []uint64) {
	t.Helper()
	got := r332LevelReverses(h, level)
	for i := 1; i < len(got); i++ {
		if got[i-1] <= got[i] {
			t.Errorf("level %d list out of order: %#x<<56 before %#x<<56", level, got[i-1]>>56, got[i]>>56)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("level %d list = %s, want %s", level, r332Hex(got), r332Hex(want))
	}
}

// r332Claim claims the bucket of reverse from the pool and activates it, as
// makeBucket does, without linking it into the list of buckets.
func r332Claim(t *testing.T, h *Map, reverse uint64) {
	t.Helper()
	b, ok := h.bucketFromPool(reverse, useOnOk(true))
	if b == nil {
		t.Errorf("bucketFromPool(%#x<<56) = nil", reverse>>56)
		return
	}
	if ok != nil {
		ok()
	}
}

// r332Keys returns n keys whose reversed hashes have top as their top 8
// bits, in the order of the reversed hashes.
func r332Keys(top uint64, n int) []string {
	var keys []string
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("r332-%d", i)
		h, _ := KeyToHash(k)
		if bits.Reverse64(h)>>56 == top {
			keys = append(keys, k)
		}
	}
	if n == 2 {
		a, _ := KeyToHash(keys[0])
		b, _ := KeyToHash(keys[1])
		if bits.Reverse64(a) > bits.Reverse64(b) {
			keys[0], keys[1] = keys[1], keys[0]
		}
	}
	return keys
}

// r332SplitMap returns a map that never splits a bucket by itself, holding
// two keys whose reverses are 0x11<<56 and a bit more, and the list node of
// the larger key. makeBucket(node) splits the bucket of the smaller key: its
// new bucket is the middle of the buckets around that key.
func r332SplitMap(t *testing.T) (*Map, func() error) {
	t.Helper()
	h := New(MaxPefBucket(1 << 20))
	keys := r332Keys(0x11, 2)
	for _, k := range keys {
		if !h.Set(k, k) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	item, ok := h.LoadItem(keys[1])
	if !ok {
		t.Fatalf("LoadItem(%q) not found", keys[1])
	}
	node := item.PtrListHead()
	return h, func() error { return h.makeBucket(node, 0) }
}

// r332StopAt makes the first goroutine that passes point with arguments that
// match stop there until release is closed. reached is closed when it stops.
func r332StopAt(t *testing.T, point string, match func(a, b unsafe.Pointer) bool) (reached chan struct{}, release func()) {
	reached, ch := make(chan struct{}), make(chan struct{})
	var once, closeOnce sync.Once
	release = func() { closeOnce.Do(func() { close(ch) }) }
	SetStepHook(func(p string, a, b unsafe.Pointer) {
		if p != point || !match(a, b) {
			return
		}
		stop := false
		once.Do(func() { stop = true })
		if !stop {
			return
		}
		close(reached)
		<-ch
	})
	t.Cleanup(func() {
		release()
		SetStepHook(nil)
	})
	return reached, release
}

func r332Wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func r332Go(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

// Sequential. The level 2 list is empty after New. Claiming 0x28<<56 makes
// the first downLevels of h.buckets[2], D2 (reverse 2<<60), and links it into
// the level 2 list: [D2]. Claiming 0x18<<56 then makes D1 (reverse 1<<60).
// The walk in bucketFromPool starts at D2 and stops at once, because D2 is
// the last bucket of the list and NextOnLevel returns D2 itself; it never
// compares D1 with D2. D1 is linked before D2: [D1, D2], in ascending order.
func Test_Repro_3_3_2_FirstDownLevelSkipsLastOnLevel(t *testing.T) {
	h := New()
	r332Claim(t, h, 0x28<<56)
	r332Claim(t, h, 0x18<<56)
	r332AssertLevel(t, h, 2, []uint64{2 << 60, 1 << 60})
}

// The control of the test above: claiming 0x18<<56 first and 0x28<<56 next
// links D2 before D1, which is the right place by chance.
func Test_Repro_3_3_2_FirstDownLevelAscendingParents(t *testing.T) {
	h := New()
	r332Claim(t, h, 0x18<<56)
	r332Claim(t, h, 0x28<<56)
	r332AssertLevel(t, h, 2, []uint64{2 << 60, 1 << 60})
}

// Sequential. The first downLevels of h.buckets[1..4] are claimed in the
// order that keeps the level 2 list in order: [D4, D3, D2, D1]. A split in
// h.buckets[1] then makes the bucket 0x18<<56. findNextLevelBucket walks the
// list without comparing 0x18<<56 with any bucket above it and, after the
// loop, returns the second bucket of the list, D3, not the last one.
// makeBucket inserts before the next bucket of D3, D2: [D4, D3, 0x18, D2,
// D1].
func Test_Repro_3_3_2_FindNextLevelBucketReturnsSecond(t *testing.T) {
	h, split := r332SplitMap(t)
	r332Claim(t, h, 0x1f<<56)
	r332Claim(t, h, 0x2f<<56)
	r332Claim(t, h, 0x3f<<56)
	r332Claim(t, h, 0x4f<<56)
	r332AssertLevel(t, h, 2, []uint64{4 << 60, 3 << 60, 2 << 60, 1 << 60})
	if err := split(); err != nil {
		t.Logf("makeBucket: %v", err)
	}
	r332AssertLevel(t, h, 2, []uint64{4 << 60, 3 << 60, 2 << 60, 0x18 << 56, 1 << 60})
	if err := StepCheckLists(h); err != nil {
		t.Errorf("%v", err)
	}
}

// Concurrent, bucketFromPool against bucketFromPool. The level 2 list holds
// D1 (reverse 1<<60). G3 claims 0x38<<56: its walk ends at D1, the last
// bucket, and G3 stops at "bucketFromPool.levelFound" before it inserts D3
// before D1. G2 then claims 0x28<<56 to the end: its walk also ends at D1 and
// it links D2 before D1: [D2, D1]. When G3 resumes, the insertion reads the
// prev of D1 again, now D2, and links D3 between D2 and D1: [D2, D3, D1].
func Test_Repro_3_3_2_FirstDownLevelsInsertIntoSameGap(t *testing.T) {
	h := New()
	r332Claim(t, h, 0x18<<56)
	reached, release := r332StopAt(t, "bucketFromPool.levelFound", func(a, _ unsafe.Pointer) bool {
		return StepBucketReverse(a) == 3<<60
	})
	g3 := r332Go(func() { r332Claim(t, h, 0x38<<56) })
	r332Wait(t, reached, "G3 at bucketFromPool.levelFound")
	r332Claim(t, h, 0x28<<56)
	release()
	r332Wait(t, g3, "G3 to finish")
	r332AssertLevel(t, h, 2, []uint64{3 << 60, 2 << 60, 1 << 60})
}

// The control of the test above: G2 stops at the same point and G3 runs to
// the end first. G3 walks to D1 and links D3 before it; G2 resumes and links
// D2 before D1, after D3: [D3, D2, D1].
func Test_Repro_3_3_2_FirstDownLevelsSmallerStops(t *testing.T) {
	h := New()
	r332Claim(t, h, 0x18<<56)
	reached, release := r332StopAt(t, "bucketFromPool.levelFound", func(a, _ unsafe.Pointer) bool {
		return StepBucketReverse(a) == 2<<60
	})
	g2 := r332Go(func() { r332Claim(t, h, 0x28<<56) })
	r332Wait(t, reached, "G2 at bucketFromPool.levelFound")
	r332Claim(t, h, 0x38<<56)
	release()
	r332Wait(t, g2, "G2 to finish")
	r332AssertLevel(t, h, 2, []uint64{3 << 60, 2 << 60, 1 << 60})
}

// r332ClaimEmbedded claims the bucket of reverse from the pool of a map with
// embedded item pools, without linking it into the list of buckets.
func r332ClaimEmbedded(t *testing.T, h *Map, reverse uint64) {
	t.Helper()
	if b := h.bucketFromPoolEmbedded(reverse); b == nil {
		t.Errorf("bucketFromPoolEmbedded(%#x<<56) = nil", reverse>>56)
	}
}

// The same as Test_Repro_3_3_2_FirstDownLevelSkipsLastOnLevel, in
// bucketFromPoolEmbedded of a map with embedded item pools (skiplistmap5).
func Test_Repro_3_3_2_EmbeddedFirstDownLevelSkipsLastOnLevel(t *testing.T) {
	h := New(UseEmbeddedPool(true))
	r332ClaimEmbedded(t, h, 0x28<<56)
	r332ClaimEmbedded(t, h, 0x18<<56)
	r332AssertLevel(t, h, 2, []uint64{2 << 60, 1 << 60})
}

// The same as Test_Repro_3_3_2_FirstDownLevelsInsertIntoSameGap, in
// bucketFromPoolEmbedded: G3 stops at "bucketFromPoolEmbedded.levelFound"
// before it inserts D3 before D1, G2 links D2 before D1, and G3 links D3
// between D2 and D1: [D2, D3, D1].
func Test_Repro_3_3_2_EmbeddedFirstDownLevelsInsertIntoSameGap(t *testing.T) {
	h := New(UseEmbeddedPool(true))
	r332ClaimEmbedded(t, h, 0x18<<56)
	reached, release := r332StopAt(t, "bucketFromPoolEmbedded.levelFound", func(a, _ unsafe.Pointer) bool {
		return StepBucketReverse(a) == 3<<60
	})
	g3 := r332Go(func() { r332ClaimEmbedded(t, h, 0x38<<56) })
	r332Wait(t, reached, "G3 at bucketFromPoolEmbedded.levelFound")
	r332ClaimEmbedded(t, h, 0x28<<56)
	release()
	r332Wait(t, g3, "G3 to finish")
	r332AssertLevel(t, h, 2, []uint64{3 << 60, 2 << 60, 1 << 60})
}

// Concurrent, makeBucket against makeBucket. A first split of h.buckets[1]
// (P1) makes 0x18<<56: the level 2 list is [0x18, D1]. G1 splits the gap
// between P1 and 0x18: it makes 0x14<<56, links it into the list of buckets,
// gets D1 from findNextLevelBucket and stops at "makeBucket.levelFound"
// before it inserts 0x14 before D1. G2 then splits the gap between P1 and
// 0x14 to the end: it makes 0x12<<56, gets D1 too, because 0x14 is not in the
// level list yet, and links 0x12 before D1: [0x18, 0x12, D1]. When G1
// resumes, the insertion reads the prev of D1 again, now 0x12, and links 0x14
// between 0x12 and D1: [0x18, 0x12, 0x14, D1].
func Test_Repro_3_3_2_MakeBucketLevelInsertIntoSameGap(t *testing.T) {
	h, split := r332SplitMap(t)
	if err := split(); err != nil {
		t.Logf("first makeBucket: %v", err)
	}
	r332AssertLevel(t, h, 2, []uint64{0x18 << 56, 1 << 60})
	reached, release := r332StopAt(t, "makeBucket.levelFound", func(a, _ unsafe.Pointer) bool {
		return StepBucketReverse(a) == 0x14<<56
	})
	g1 := r332Go(func() { split() })
	r332Wait(t, reached, "G1 at makeBucket.levelFound")
	if err := split(); err != nil {
		t.Logf("G2 makeBucket: %v", err)
	}
	release()
	r332Wait(t, g1, "G1 to finish")
	r332AssertLevel(t, h, 2, []uint64{0x18 << 56, 0x14 << 56, 0x12 << 56, 1 << 60})
	if err := StepCheckLists(h); err != nil {
		t.Errorf("%v", err)
	}
}

// The sequential control of the test above: the same three splits one after
// another keep the level 2 list in order.
func Test_Repro_3_3_2_MakeBucketLevelSequential(t *testing.T) {
	h, split := r332SplitMap(t)
	for i := 0; i < 3; i++ {
		split()
	}
	r332AssertLevel(t, h, 2, []uint64{0x18 << 56, 0x14 << 56, 0x12 << 56, 1 << 60})
}

// Sequential. addBucket links the new bucket before the first smaller bucket
// and then returns ErrBucketInvalidOrder, although the list is in order: the
// check after the insertion is the negation of the condition that led to it.
// makeBucket returns that error.
func Test_Repro_3_3_2_AddBucketErrorsAfterInsert(t *testing.T) {
	h, split := r332SplitMap(t)
	err := split()
	if cerr := StepCheckLists(h); cerr != nil {
		t.Errorf("%v", cerr)
	}
	if got := r332BucketReverses(h); !r332Has(got, 0x18<<56) {
		t.Errorf("list of buckets %s does not hold 0x18<<56", r332Hex(got))
	}
	if err != nil {
		t.Errorf("makeBucket() = %v after the bucket was linked in order", err)
	}
}

func r332Has(rs []uint64, r uint64) bool {
	for _, v := range rs {
		if v == r {
			return true
		}
	}
	return false
}

// r332Gid returns the id of the calling goroutine.
func r332Gid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	id, _ := strconv.ParseInt(s[:strings.IndexByte(s, ' ')], 10, 64)
	return id
}

type r332Event struct {
	g     int
	point string
	done  bool
	err   error
}

// r332Run runs a few goroutines one step at a time. A step runs one goroutine
// from a step point to the next one; the points are those of this package and
// those of lista (prefixed by "lista."). A goroutine is stepped from its start
// to "makeBucket.levelFound" and runs freely after it, because it does not
// touch the list of buckets after that point.
type r332Run struct {
	mu     sync.Mutex
	owner  map[int64]int
	free   []bool
	events chan r332Event
	resume []chan struct{}
	trace  []string
}

// r332Points are the step points the explorer stops at. They are fixed, so
// that a point added to the map or lista later does not multiply the
// schedules to play.
var r332Points = map[string]bool{
	"add2.found": true, "makeBucket.begin": true, "makeBucket.pairFound": true,
	"makeBucket.claimed": true, "bucketFromPool.lenStored": true,
	"bucketFromPool.levelFound": true, "insertBucket.begin": true,
	"insertBucket.dummyLinked": true, "makeBucket.levelFound": true,
	"lista.insert.begin": true, "lista.add.cas1": true, "lista.add.cas2": true,
	"lista.add.rollback": true, "lista.del.begin": true,
	"lista.del.nextMarked": true, "lista.del.marked": true,
}

func (r *r332Run) step(point string) {
	id := r332Gid()
	r.mu.Lock()
	g, ok := r.owner[id]
	if ok && !r332Points[point] {
		ok = false
	}
	if ok && r.free[g] {
		ok = false
	}
	if ok && point == "makeBucket.levelFound" {
		r.free[g] = true
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	r.events <- r332Event{g: g, point: point}
	<-r.resume[g]
}

// play runs ops following the schedule prefix and then always picks the first
// goroutine that can go on. It returns the choices it made, each as the index
// picked and the number of goroutines that could go on.
func (r *r332Run) play(t *testing.T, ops []func() error, prefix []int) (choices [][2]int, errs []error, ok bool) {
	r.owner = map[int64]int{}
	r.free = make([]bool, len(ops))
	r.events = make(chan r332Event)
	r.resume = make([]chan struct{}, len(ops))
	r.trace = nil
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
			r.free[g] = true
			r.mu.Unlock()
			r.events <- r332Event{g: g, done: true, err: err}
		}(g)
	}
	parked := make([]bool, len(ops))
	finished := make([]bool, len(ops))
	wait := func() bool {
		select {
		case ev := <-r.events:
			if ev.done {
				finished[ev.g], errs[ev.g] = true, ev.err
				r.trace = append(r.trace, fmt.Sprintf("G%d returns %v", ev.g+1, ev.err))
			} else {
				parked[ev.g] = true
				r.trace = append(r.trace, fmt.Sprintf("G%d at %s", ev.g+1, ev.point))
			}
			return true
		case <-time.After(5 * time.Second):
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Errorf("a goroutine did not reach a step point\n%s\n%s", strings.Join(r.trace, "\n"), buf[:n])
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
		parked[g] = false
		r.resume[g] <- struct{}{}
		if !wait() {
			return nil, nil, false
		}
		if len(choices) > 400 {
			t.Errorf("schedule does not end\n%s", strings.Join(r.trace, "\n"))
			return nil, nil, false
		}
	}
	return choices, errs, true
}

// r332NextPrefix returns the schedule after choices in depth first order, or
// nil when every schedule is played.
func r332NextPrefix(choices [][2]int) []int {
	for i := len(choices) - 1; i >= 0; i-- {
		if choices[i][0]+1 < choices[i][1] {
			prefix := make([]int, i+1)
			for j := 0; j < i; j++ {
				prefix[j] = choices[j][0]
			}
			prefix[i] = choices[i][0] + 1
			return prefix
		}
	}
	return nil
}

// r332BucketReverses returns the reverses of the list of buckets, walked
// forward from its head.
func r332BucketReverses(h *Map) []uint64 {
	var got []uint64
	for cur := h.headBucket.DirectNext(); cur != h.tailBucket && len(got) < 1000; cur = cur.DirectNext() {
		got = append(got, bucketFromListHead(cur).reverse)
	}
	return got
}

// r332SplitNode stores two keys whose reverses have top as their top 8 bits
// and returns them with a split: makeBucket of the list node of the larger
// key splits the bucket of the smaller key.
func r332SplitNode(t *testing.T, h *Map, top uint64) ([]string, func() error) {
	t.Helper()
	keys := r332Keys(top, 2)
	for _, k := range keys {
		if !h.Set(k, k) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	item, ok := h.LoadItem(keys[1])
	if !ok {
		t.Fatalf("LoadItem(%q) not found", keys[1])
	}
	node := item.PtrListHead()
	return keys, func() error { return h.makeBucket(node, 0) }
}

// Evidence that two splits do not break the order of the list of buckets.
// The map has split h.buckets[1] (P1) once, into P1 and 0x18<<56. G1 splits
// the bucket of a key at 0x11<<56: it makes 0x14<<56 and links it before P1.
// G2 splits one of:
//   - the bucket of the same key: G2 makes 0x14<<56 too and loses the claim
//     of it, or makes 0x12<<56 after G1 linked 0x14<<56;
//   - the bucket of a key at 0x13<<56: the same;
//   - the bucket of a key at 0x19<<56: G2 makes 0x1c<<56 before 0x18<<56
//     (Test_Repro_3_3_2_ExploreBucketListSplitsApart).
//
// Every interleaving of the two at the step points from their start to
// "makeBucket.levelFound" is played: makeBucket.pairFound (the pair of
// buckets read, before the claim), makeBucket.claimed, insertBucket.begin
// (after the walk of addBucket), insertBucket.dummyLinked, and the lista
// points insert.begin, add.cas1 and add.cas2 (between the two CASes). After
// each one the list of entries and the list of buckets must be in order, and
// every key must be found. The level 2 list is only counted: it breaks in
// some of them (Test_Repro_3_3_2_MakeBucketLevelInsertIntoSameGap).
func Test_Repro_3_3_2_ExploreBucketListSplits(t *testing.T) {
	r332ExploreSplits(t, 0x11, 0x13)
}


func r332ExploreSplits(t *testing.T, g2tops ...uint64) {
	for _, g2top := range g2tops {
		g2top := g2top
		t.Run(fmt.Sprintf("G2at%#x", g2top), func(t *testing.T) {
			var prefix []int
			played, levelBroken := 0, 0
			finals := map[string]int{}
			for {
				h := New(MaxPefBucket(1 << 20))
				keys, g1 := r332SplitNode(t, h, 0x11)
				g2 := g1
				if g2top != 0x11 {
					var more []string
					more, g2 = r332SplitNode(t, h, g2top)
					keys = append(keys, more...)
				}
				g1()
				var r r332Run
				choices, errs, ok := r.play(t, []func() error{g1, g2}, prefix)
				if !ok {
					return
				}
				played++
				got := r332BucketReverses(h)
				finals[r332Hex(got)]++
				fail := func(format string, args ...interface{}) {
					t.Errorf("schedule %v: %s\nerrors %v\n%s", choices, fmt.Sprintf(format, args...), errs, strings.Join(r.trace, "\n"))
				}
				if err := StepCheckLists(h); err != nil {
					fail("%v", err)
				}
				if !r332Has(got, 0x14<<56) {
					fail("list of buckets %s does not hold 0x14<<56", r332Hex(got))
				}
				for _, k := range keys {
					if _, found := h.Get(k); !found {
						fail("Get(%q) not found", k)
					}
				}
				lr := r332LevelReverses(h, 2)
				for i := 1; i < len(lr); i++ {
					if lr[i-1] <= lr[i] {
						levelBroken++
						break
					}
				}
				if t.Failed() {
					return
				}
				if prefix = r332NextPrefix(choices); prefix == nil {
					break
				}
			}
			t.Logf("%d schedules played; the level 2 list is out of order in %d of them", played, levelBroken)
			for k, n := range finals {
				t.Logf("list of buckets %s in %d schedules", k, n)
			}
		})
	}
}
