//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// stepper stops goroutines at the step points of elist_head and skiplistmap,
// so that a test replays a concurrent interleaving one step at a time.
type stepper struct {
	mu    sync.Mutex
	stops []*stepStop
	hits  map[stepHit]int
	last  map[string][2]unsafe.Pointer
}

type stepHit struct {
	point string
	a     unsafe.Pointer
}

type stepStop struct {
	point   string
	match   func(a, b, c unsafe.Pointer) bool
	used    bool
	a, b    unsafe.Pointer // the arguments of the stopped goroutine
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newStepper(t *testing.T) *stepper {
	t.Helper()
	s := &stepper{hits: map[stepHit]int{}, last: map[string][2]unsafe.Pointer{}}
	elist_head.SetStepHook(func(point string, a, b, c *elist_head.ListHead) {
		s.at("elist."+point, unsafe.Pointer(a), unsafe.Pointer(b), unsafe.Pointer(c))
	})
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		s.at("map."+point, a, b, nil)
	})
	t.Cleanup(func() {
		elist_head.SetStepHook(nil)
		skiplistmap.SetStepHook(nil)
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, st := range s.stops {
			st.Release()
		}
	})
	return s
}

// stopAt stops the first goroutine that reaches point with arguments for
// which match returns true, until Release is called.
func (s *stepper) stopAt(point string, match func(a, b, c unsafe.Pointer) bool) *stepStop {
	st := &stepStop{point: point, match: match, reached: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	s.stops = append(s.stops, st)
	s.mu.Unlock()
	return st
}

// args returns the arguments of the last time point was reached.
func (s *stepper) args(point string) (a, b unsafe.Pointer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.last[point]
	return l[0], l[1]
}

// count returns how many times point was reached with first argument a.
func (s *stepper) count(point string, a unsafe.Pointer) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[stepHit{point, a}]
}

// total returns how many times point was reached.
func (s *stepper) total(point string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for h, c := range s.hits {
		if h.point == point {
			n += c
		}
	}
	return n
}

func (s *stepper) at(point string, a, b, c unsafe.Pointer) {
	s.mu.Lock()
	s.hits[stepHit{point, a}]++
	s.last[point] = [2]unsafe.Pointer{a, b}
	var st *stepStop
	for _, x := range s.stops {
		if !x.used && x.point == point && (x.match == nil || x.match(a, b, c)) {
			x.used = true
			x.a, x.b = a, b
			st = x
			break
		}
	}
	s.mu.Unlock()
	if st == nil {
		return
	}
	close(st.reached)
	<-st.release
}

func (st *stepStop) Release() {
	st.once.Do(func() { close(st.release) })
}

// waitReached fails the test unless a goroutine stops at st before done is closed.
func (st *stepStop) waitReached(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-st.reached:
	case <-done:
		t.Fatalf("finished without reaching %s", st.point)
	case <-time.After(10 * time.Second):
		t.Fatalf("no goroutine reached %s", st.point)
	}
}

func isNode(p unsafe.Pointer) func(a, b, c unsafe.Pointer) bool {
	return func(a, b, c unsafe.Pointer) bool { return a == p }
}

func isSecond(p unsafe.Pointer) func(a, b, c unsafe.Pointer) bool {
	return func(a, b, c unsafe.Pointer) bool { return b == p }
}

// goStep runs fn in a new goroutine and returns a channel closed when fn
// returns. A panic in fn is reported as a test error.
func goStep(t *testing.T, fn func()) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic: %v", r)
			}
		}()
		fn()
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s did not finish\n%s", what, buf[:n])
	}
}

// newStepMap returns a map without the embedded pool that never splits a
// bucket, so that the tests below see only the insertions they make.
func newStepMap() *WrapHMap {
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](1 << 20)(m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m.base)
	return m
}

func reverseOf(key string) uint64 {
	k, _ := skiplistmap.KeyToHash(key)
	return bits.Reverse64(k)
}

// adjacentKeys returns n keys in the order of their reversed hashes. The keys
// share the top 4 bits of the reversed hash, so no bucket dummy lies between
// them in the list of a map that does not split buckets.
func adjacentKeys(n int) []string {
	groups := map[uint64][]string{}
	for i := 0; ; i++ {
		k := crashKey(i)
		top := reverseOf(k) >> 60
		groups[top] = append(groups[top], k)
		if g := groups[top]; len(g) == n {
			sort.Slice(g, func(i, j int) bool { return reverseOf(g[i]) < reverseOf(g[j]) })
			return g
		}
	}
}

// regionKeys returns n keys whose reversed hashes have top as their top 4
// bits and second as their next 4 bits, in the order of the reversed hashes.
func regionKeys(top, second uint64, n int) []string {
	var keys []string
	for i := 0; len(keys) < n; i++ {
		k := crashKey(i)
		if r := reverseOf(k); r>>60 == top && (r>>56)&0xf == second {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })
	return keys
}

func newStepItems(keys []string) []skiplistmap.SampleItem[skiplistmap.StringKey, any] {
	items := make([]skiplistmap.SampleItem[skiplistmap.StringKey, any], len(keys))
	for i := range items {
		items[i].InitEntry(skiplistmap.StringKey(keys[i]), &list_head.ListHead{})
	}
	return items
}

func nodeOf[T interface{ PtrListHead() *elist_head.ListHead }](item T) unsafe.Pointer {
	return unsafe.Pointer(item.PtrListHead())
}

// assertStoredInOrder checks that every key is found, and that the list
// holds exactly the keys in the order of their reversed hashes.
func assertStoredInOrder(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](m.base); err != nil {
		t.Errorf("%v", err)
	}
	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
	var got []string
	runWithDeadline(t, 10*time.Second, func() {
		m.base.RangeItemForTest(func(item *skiplistmap.TestEntry[skiplistmap.StringKey, any]) bool {
			got = append(got, string(item.Key()))
			return true
		})
	})
	for i := 1; i < len(got); i++ {
		if reverseOf(got[i-1]) > reverseOf(got[i]) {
			t.Errorf("list order broken: %q before %q", got[i-1], got[i])
		}
	}
	if len(got) != len(keys) {
		t.Errorf("list holds %d keys %q, want %d %q", len(got), got, len(keys), keys)
	}
	if n := m.base.Len(); n != len(keys) {
		t.Errorf("Len() = %d, want %d", n, len(keys))
	}
}

// Keys a < b < c < d. The list holds a and d. Storing b stops after add2
// found d as its position; storing c then links c between a and d. When b
// resumes, the order check sees c before d and rejects b: b must still be
// stored, before c.
func Test_StepAdd2ResumesAfterRejectedPosition(t *testing.T) {
	keys := adjacentKeys(4)
	items := newStepItems(keys)
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("map.add2.found", isNode(nodeOf(&items[1])))
	done := goStep(t, func() { m.base.StoreItem(&items[1]) })
	stop.waitReached(t, done)
	m.base.StoreItem(&items[2])
	stop.Release()
	waitDone(t, done, "StoreItem(b)")

	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}

// Keys a < b < c < d. The list holds a and d. Storing b passes the order
// check with a before d and stops before its first CAS; storing c then links
// c between a and d. When b resumes, its CAS fails: b must be linked between
// a and c, not after c.
func Test_StepRetryAfterFailedCASKeepsOrder(t *testing.T) {
	keys := adjacentKeys(4)
	items := newStepItems(keys)
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("elist.add.cas1", isNode(nodeOf(&items[1])))
	done := goStep(t, func() { m.base.StoreItem(&items[1]) })
	stop.waitReached(t, done)
	m.base.StoreItem(&items[2])
	stop.Release()
	waitDone(t, done, "StoreItem(b)")

	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}

// Keys a < b < c < d. The list holds a and d. Storing b passes the order
// check with a before d and stops before InsertBefore reads the previous
// node of d; storing c then links c between a and d. When b resumes, it reads
// c as the previous node: b must be linked between a and c, not after c.
func Test_StepInsertReadsPrevAfterOrderCheck(t *testing.T) {
	keys := adjacentKeys(4)
	items := newStepItems(keys)
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("elist.insert.begin", isNode(nodeOf(&items[1])))
	done := goStep(t, func() { m.base.StoreItem(&items[1]) })
	stop.waitReached(t, done)
	m.base.StoreItem(&items[2])
	stop.Release()
	waitDone(t, done, "StoreItem(b)")

	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}

// Keys a < b < c < d. The list holds a and d. Storing b stops between its two
// CASes: a.next is b, d.prev is still a. Storing c reads a as the previous
// node of d and fails its CAS until b finishes. c must be stored after b
// resumes, not dropped.
func Test_StepInsertNextToUnfinishedInsert(t *testing.T) {
	keys := adjacentKeys(4)
	items := newStepItems(keys)
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("elist.add.cas2", isNode(nodeOf(&items[1])))
	doneB := goStep(t, func() { m.base.StoreItem(&items[1]) })
	stop.waitReached(t, doneB)
	doneC := goStep(t, func() { m.base.StoreItem(&items[2]) })
	// b resumes only after c either gave up or retried more often than the
	// 100 retries of InsertBefore.
	deadline := time.Now().Add(10 * time.Second)
	for s.count("elist.add.cas1", nodeOf(&items[2])) <= 100 {
		select {
		case <-doneC:
		default:
			if time.Now().After(deadline) {
				t.Fatalf("c neither finished nor retried")
			}
			runtime.Gosched()
			continue
		}
		break
	}
	stop.Release()
	waitDone(t, doneB, "StoreItem(b)")
	waitDone(t, doneC, "StoreItem(c)")

	assertStoredInOrder(t, m, keys)
	runtime.KeepAlive(items)
}

// Two goroutines split the same bucket at the same time. The one that loses
// the CAS on the state of the new bucket must not initialize the bucket or its
// dummy again. In this interleaving the loser initializes the dummy after the
// winner linked it.
//
// A first split of the region at a higher position leaves the slot of the
// second split in downLevels unclaimed, so that both goroutines take the
// state CAS path of bucketFromPool for it.
func Test_StepSplitLoserKeepsWinnersDummy(t *testing.T) {
	const top = 0x3
	upper := regionKeys(top, 0xc, 8)
	lower := regionKeys(top, 0x0, 16)
	items := newStepItems(append(append([]string{}, upper...), lower...))
	upperItems, lowerItems := items[:len(upper)], items[len(upper):]
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](2)(m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m.base)

	s := newStepper(t)
	var stored []string
	for i := range upperItems {
		m.base.StoreItem(&upperItems[i])
		stored = append(stored, string(upperItems[i].Key()))
		if s.total("map.makeBucket.claimed") > 0 {
			break
		}
	}
	first, _ := s.args("map.makeBucket.claimed")
	if first == nil {
		t.Fatalf("storing %d keys did not split the region", len(stored))
	}
	t.Logf("first split: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](first))

	// The first lower key whose store splits the bucket stops as the winner.
	var stop1 *stepStop
	var done1 <-chan struct{}
	next := 0
	for ; next < len(lowerItems); next++ {
		st := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&lowerItems[next])))
		d := goStep(t, func(it *skiplistmap.SampleItem[skiplistmap.StringKey, any]) func() {
			return func() { m.base.StoreItem(it) }
		}(&lowerItems[next]))
		stored = append(stored, string(lowerItems[next].Key()))
		select {
		case <-st.reached:
			stop1, done1 = st, d
		case <-d:
		case <-time.After(10 * time.Second):
			t.Fatalf("StoreItem(%q) neither split nor finished", string(lowerItems[next].Key()))
		}
		if stop1 != nil {
			next++
			break
		}
	}
	if stop1 == nil {
		t.Fatalf("no lower key split the bucket")
	}
	b1 := stop1.a
	t.Logf("second split: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](b1))

	// The next lower key splits the same bucket.
	stop2 := s.stopAt("map.makeBucket.claimed", isSecond(nodeOf(&lowerItems[next])))
	done2 := goStep(t, func() { m.base.StoreItem(&lowerItems[next]) })
	stored = append(stored, string(lowerItems[next].Key()))
	stop2.waitReached(t, done2)
	switch b2 := stop2.a; {
	case b2 == nil:
		// The loser leaves the split to the winner.
		stop2.Release()
		waitDone(t, done2, "loser")
		stop1.Release()
		waitDone(t, done1, "winner")
	case b2 == b1:
		// The loser runs up to the initialization of the dummy, the winner
		// links the dummy, and then the loser initializes it.
		beforeInit := s.stopAt("map.insertBucket.begin", isNode(b1))
		stop2.Release()
		beforeInit.waitReached(t, done2)
		linked := s.stopAt("map.insertBucket.dummyLinked", isNode(b1))
		stop1.Release()
		linked.waitReached(t, done1)
		beforeInit.Release()
		waitDone(t, done2, "loser")
		linked.Release()
		waitDone(t, done1, "winner")
	default:
		t.Fatalf("the second store split another bucket: %016x", skiplistmap.StepBucketReverse[skiplistmap.StringKey, any](b2))
	}

	assertStoredInOrder(t, m, stored)
	runtime.KeepAlive(items)
}

// storeUntilStop stores items one by one, each in a new goroutine, until one
// of them stops at st. It returns the keys stored so far, the index of the
// item after the stopped one, and the channel closed when the stopped store
// finishes. It fails the test unless an item other than the last one stops.
func storeUntilStop(t *testing.T, m *WrapHMap, items []skiplistmap.SampleItem[skiplistmap.StringKey, any], st *stepStop) ([]string, int, <-chan struct{}) {
	return storeTypedUntilStop(t, m.base, items, st)
}

func storeTypedUntilStop[K skiplistmap.Key[K]](t *testing.T, m *skiplistmap.Map[K, any], items []skiplistmap.SampleItem[K, any],

	st *stepStop) (stored []string, next int, done <-chan struct{}) {
	t.Helper()
	for ; next < len(items) && done == nil; next++ {
		d := goStep(t, func(it *skiplistmap.SampleItem[K, any]) func() {
			return func() { m.StoreItem(it) }
		}(&items[next]))
		stored = append(stored, fmt.Sprint(items[next].Key()))
		select {
		case <-st.reached:
			done = d
		case <-d:
		case <-time.After(10 * time.Second):
			t.Fatalf("StoreItem(%q) neither reached %s nor finished", fmt.Sprint(items[next].Key()), st.point)
		}
	}
	if done == nil || next == len(items) {
		t.Fatalf("storing %d keys reached %s only at the last key or never", len(stored), st.point)
	}
	return stored, next, done
}

// Two goroutines make the first split of a region at the same time. The one
// that loses the CAS on cntOfActiveLevels waits until the winner has stored
// the new downLevels; once it sees their length it must also see their
// capacity. In this interleaving the winner stops right after storing the
// length.
func Test_StepSplitSeesDownLevelsCapacity(t *testing.T) {
	const top = 0x3
	keys := regionKeys(top, 0xc, 8)
	items := newStepItems(keys)
	m := newWrapHMap(skiplistmap.NewHMap[skiplistmap.StringKey, any]())
	skiplistmap.MaxPefBucket[skiplistmap.StringKey, any](2)(m.base)
	skiplistmap.BucketMode[skiplistmap.StringKey, any](skiplistmap.CombineSearch4)(m.base)

	s := newStepper(t)
	stop := s.stopAt("map.bucketFromPool.lenStored", nil)
	stored, next, doneW := storeUntilStop(t, m, items, stop)

	doneL := goStep(t, func() { m.base.StoreItem(&items[next]) })
	stored = append(stored, string(items[next].Key()))
	waitDone(t, doneL, "loser")
	stop.Release()
	waitDone(t, doneW, "winner")

	assertStoredInOrder(t, m, stored)
	runtime.KeepAlive(items)
}
