//go:build stephook

package skiplistmap

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/elist_head"
)

type poolLookupKey uint64

func (k poolLookupKey) KeyHash() (uint64, uint64)      { return bits.Reverse64(uint64(k)), 1 }
func (k poolLookupKey) Equal(other poolLookupKey) bool { return k == other }

func TestPoolInsertRetriesOldCandidate(t *testing.T) {
	for _, operation := range []string{"Get", "Delete"} {
		t.Run(operation, func(t *testing.T) {
			m := New[poolLookupKey, int](UseEmbeddedPool[poolLookupKey, int](true), MaxPefBucket[poolLookupKey, int](512))
			for i := uint64(2); i <= 16; i += 2 {
				if !m.Set(poolLookupKey(i<<40), int(i)) {
					t.Fatal("initial Set")
				}
			}
			key := poolLookupKey(4 << 40)
			pool := m.findBucket(uint64(key)).toBase().itemPool()
			old := pool.items
			candidate := m.bsearchBybucket(m.findBucket(uint64(key)), uint64(key), true)
			if old.Len() != 8 || candidate != old.at(1) {
				t.Fatal("candidate must be an interior pool entry")
			}
			reached, resume := make(chan struct{}), make(chan struct{})
			done := make(chan bool, 1)
			var stopped atomic.Bool
			var release sync.Once
			pointToStop := "lookup.pool.candidate"
			if operation == "Get" {
				pointToStop = "get.beforeValue"
			}
			SetStepHook(func(point string, a, _ unsafe.Pointer) {
				if point == pointToStop && a == unsafe.Pointer(candidate.PtrListHead()) && stopped.CompareAndSwap(false, true) {
					close(reached)
					<-resume
				}
			})
			t.Cleanup(func() { release.Do(func() { close(resume) }); SetStepHook(nil) })
			go func() {
				if operation == "Delete" {
					done <- m.Delete(key)
					return
				}
				value, ok := m.Get(key)
				done <- ok && value == 4
			}()
			select {
			case <-reached:
			case <-time.After(time.Second):
				t.Fatal("lookup did not reach candidate")
			}
			if !m.Set(poolLookupKey(15<<40), 15) {
				t.Fatal("middle insertion")
			}
			if !candidate.IsIgnored() || candidate.ListHead.IsMarked() {
				t.Fatal("expected retired interior with untouched links")
			}
			release.Do(func() { close(resume) })
			select {
			case ok := <-done:
				if !ok {
					t.Fatal("lookup lost existing key after pool insertion")
				}
			case <-time.After(time.Second):
				t.Fatal("lookup did not resume")
			}
			runtime.KeepAlive(old)
		})
	}
}

func TestPoolInsertKeepsInteriorLinks(t *testing.T) {
	p, ends := makeInsertPool(8, 16)
	old := p.items
	interior := old.at(1)
	links := interior.ListHead
	reached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	elist_head.SetStepHook(func(point string, a, _, _ *elist_head.ListHead) {
		if point == "move.marked" && a == &old.at(0).ListHead {
			close(reached)
			<-resume
		}
	})
	t.Cleanup(func() {
		release.Do(func() { close(resume) })
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("insertion did not finish")
		}
		elist_head.SetStepHook(nil)
	})
	go func() {
		p.insertToPool(9, nil)
		close(done)
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("replacement did not reach its boundary publication")
	}
	if interior.ListHead != links || interior.ListHead.IsMarked() {
		t.Fatal("interior source links changed during replacement")
	}
	if interior.Key() != 1 || interior.Value() != 1 {
		t.Fatal("reader lost its old payload during replacement")
	}
	release.Do(func() { close(resume) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish")
	}
	if interior.ListHead != links || p.items.at(1).ListHead != links {
		t.Fatal("interior links were rewritten instead of copied unchanged")
	}
	if !interior.IsDeleted() || p.items.at(1).IsDeleted() {
		t.Fatal("retirement did not distinguish old and current entries")
	}
	runtime.KeepAlive(ends)
	runtime.KeepAlive(old)
}
