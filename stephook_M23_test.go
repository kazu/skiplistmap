//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
)

// m23Stop stops the first goroutine that reaches point with arguments for
// which match returns true, until Release is called. point is the name of a
// StepHook point of this package, elist or lista, prefixed with "map.",
// "elist." or "lista.".
type m23Stop struct {
	point   string
	match   func(a, b, c unsafe.Pointer) bool
	used    bool
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func m23At(point string, match func(a, b, c unsafe.Pointer) bool) *m23Stop {
	return &m23Stop{point: point, match: match, reached: make(chan struct{}), release: make(chan struct{})}
}

func (st *m23Stop) Release() {
	st.once.Do(func() { close(st.release) })
}

// waitReached fails the test unless a goroutine stops at st before done is
// closed.
func (st *m23Stop) waitReached(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-st.reached:
	case <-done:
		t.Fatalf("finished without reaching %s", st.point)
	case <-time.After(10 * time.Second):
		t.Fatalf("no goroutine reached %s", st.point)
	}
}

// m23Hooks installs the step hooks of this package, elist and lista so that
// they stop at stops, each stop once, in the order they are given when more
// than one matches. The hooks are removed and every stop is released when the
// test ends.
func m23Hooks(t *testing.T, stops ...*m23Stop) {
	var mu sync.Mutex
	at := func(point string, a, b, c unsafe.Pointer) {
		mu.Lock()
		var st *m23Stop
		for _, x := range stops {
			if !x.used && x.point == point && (x.match == nil || x.match(a, b, c)) {
				x.used = true
				st = x
				break
			}
		}
		mu.Unlock()
		if st == nil {
			return
		}
		close(st.reached)
		<-st.release
	}
	SetStepHook(func(point string, a, b unsafe.Pointer) { at("map."+point, a, b, nil) })
	elist_head.SetStepHook(func(point string, a, b, c *elist_head.ListHead) {
		at("elist."+point, unsafe.Pointer(a), unsafe.Pointer(b), unsafe.Pointer(c))
	})
	list_head.SetStepHook(func(point string, a, b, c *list_head.ListHead) {
		at("lista."+point, unsafe.Pointer(a), unsafe.Pointer(b), unsafe.Pointer(c))
	})
	t.Cleanup(func() {
		SetStepHook(nil)
		elist_head.SetStepHook(nil)
		list_head.SetStepHook(nil)
		for _, st := range stops {
			st.Release()
		}
	})
}

// m23Go runs fn in a new goroutine and returns a channel closed when fn
// returns, and where a panic in fn is stored before that, as its value
// followed by the stack of the goroutine.
func m23Go(fn func()) (done <-chan struct{}, panicked *any) {
	ch := make(chan struct{})
	var p any
	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				p = fmt.Sprintf("%v\n%s", r, debug.Stack())
			}
		}()
		fn()
	}()
	return ch, &p
}

func m23Wait(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s did not finish\n%s", what, buf[:n])
	}
}

// m23InCall reports whether the calling goroutine is inside a call of the
// function whose full name ends with name.
func m23InCall(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs)])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

// m23SplitMap returns split of r332SplitMap after running it once. The first
// split makes the bucket 0x18<<56. The next two splits make 0x14<<56 and
// 0x12<<56 and do not wait for each other in bucketFromPool, so two
// goroutines can run them at the same time. Without the first split, the
// second of two splits at the same time waits in bucketFromPool until the
// first returns.
func m23SplitMap(t *testing.T) func() error {
	t.Helper()
	_, split := r332SplitMap(t)
	if err := split(); err != nil {
		t.Logf("first makeBucket: %v", err)
	}
	return split
}

// m23KeyWithTop returns a key whose reversed hash has top as its top 4 bits.
func m23KeyWithTop(top uint64) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("m23-%d", i)
		h, _ := KeyToHash(k)
		if bits.Reverse64(h)>>60 == top {
			return k
		}
	}
}

// m23StoredNode stores key in h and returns the list node of its item.
func m23StoredNode(t *testing.T, h *Map[StringKey, any],

	key string) unsafe.Pointer {
	t.Helper()
	if !h.Set(StringKey(key), key) {
		t.Fatalf("Set(%q) failed", key)
	}
	item, ok := h.LoadItemForTest(StringKey(key))
	if !ok {
		t.Fatalf("LoadItem(%q) not found", key)
	}
	return unsafe.Pointer(item.PtrListHead())
}

func m23IsNode(p unsafe.Pointer) func(a, b, c unsafe.Pointer) bool {
	return func(a, b, c unsafe.Pointer) bool { return a == p }
}

// m23ElistMode returns the traversal mode that elist shares across the
// process. SharedTrav returns the options that put the mode back; the first
// one is applied to a fresh ModeTraverse to read the old mode, and then all
// of them to elist to leave its mode as it was.
func m23ElistMode() list_head.TraverseType {
	prevs := elist_head.SharedTrav(list_head.Direct())
	m := list_head.NewTraverse()
	prevs[0](m)
	elist_head.SharedTrav(prevs...)
	return m.Type()
}

// m23DirectModes sets the traversal modes of lista and elist, each one
// variable for the whole process, to Direct, and puts them back to Direct
// when the test ends so that a mode left behind does not reach other tests.
func m23DirectModes(t *testing.T) {
	t.Helper()
	list_head.DefaultModeTraverse.SetType(list_head.TravDirect)
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() {
		list_head.DefaultModeTraverse.SetType(list_head.TravDirect)
		elist_head.SharedTrav(list_head.Direct())
	})
}
