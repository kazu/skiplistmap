//go:build stephook

package skiplistmap_test

import (
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// stepper stops goroutines at the step points of elist_head and skiplistmap,
// so that a test replays a concurrent interleaving one step at a time.
type stepper struct {
	mu    sync.Mutex
	stops []*stepStop
	hits  map[stepHit]int
}

type stepHit struct {
	point string
	a     unsafe.Pointer
}

type stepStop struct {
	point   string
	match   func(a, b, c unsafe.Pointer) bool
	used    bool
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newStepper(t *testing.T) *stepper {
	t.Helper()
	s := &stepper{hits: map[stepHit]int{}}
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

// count returns how many times point was reached with first argument a.
func (s *stepper) count(point string, a unsafe.Pointer) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[stepHit{point, a}]
}

func (s *stepper) at(point string, a, b, c unsafe.Pointer) {
	s.mu.Lock()
	s.hits[stepHit{point, a}]++
	var st *stepStop
	for _, x := range s.stops {
		if !x.used && x.point == point && (x.match == nil || x.match(a, b, c)) {
			x.used = true
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
	m := newWrapHMap(skiplistmap.NewHMap())
	skiplistmap.MaxPefBucket(1 << 20)(m.base)
	skiplistmap.BucketMode(skiplistmap.CombineSearch4)(m.base)
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

func newStepItems(keys []string) []skiplistmap.SampleItem {
	items := make([]skiplistmap.SampleItem, len(keys))
	for i := range items {
		items[i].K = keys[i]
		items[i].SetValue(&list_head.ListHead{})
	}
	return items
}

func nodeOf(item *skiplistmap.SampleItem) unsafe.Pointer {
	return unsafe.Pointer(item.PtrListHead())
}

// assertStoredInOrder checks that every key is found, and that the list
// holds exactly the keys in the order of their reversed hashes.
func assertStoredInOrder(t *testing.T, m *WrapHMap, keys []string) {
	t.Helper()
	if err := skiplistmap.StepCheckLists(m.base); err != nil {
		t.Errorf("%v", err)
	}
	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
	var got []string
	runWithDeadline(t, 10*time.Second, func() {
		m.base.RangeItem(func(item skiplistmap.MapItem) bool {
			got = append(got, item.Key().(string))
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
