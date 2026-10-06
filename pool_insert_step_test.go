//go:build stephook

package skiplistmap

import (
	"fmt"
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

// The two ranges model adjacent pools after a split: each writer owns its
// range, while their boundary list operations can overlap.
func TestPoolMoveAdjacentBlocks(t *testing.T) {
	points := []string{"block.insert.cas2", "block.delete.forward", "del.marked", "block.delete.first"}
	for _, leftPoint := range points {
		for _, rightPoint := range points {
			t.Run(fmt.Sprintf("left=%s/right=%s", leftPoint, rightPoint), func(t *testing.T) {
				p, ends := makeInsertPool(8, 8)
				old, fresh := *p.ptrItems(), newPoolItems[IntKey, int, embeddedEntry[IntKey, int]](8, 8, true)
				var reached, resume, done [2]chan struct{}
				var released [2]sync.Once
				for i := range reached {
					reached[i], resume[i], done[i] = make(chan struct{}), make(chan struct{}), make(chan struct{})
				}
				elist_head.SetStepHook(func(point string, a, _, _ *elist_head.ListHead) {
					for side, stop := range []string{leftPoint, rightPoint} {
						node := &old.at(side * 4).ListHead
						if stop == "block.insert.cas2" {
							node = &fresh.at(side * 4).ListHead
						}
						if point == stop && a == node {
							close(reached[side])
							<-resume[side]
						}
					}
				})
				t.Cleanup(func() {
					for i := range resume {
						released[i].Do(func() { close(resume[i]) })
					}
					for _, ch := range done {
						select {
						case <-ch:
						case <-time.After(time.Second):
							t.Error("pool move did not finish")
						}
					}
					elist_head.SetStepHook(nil)
				})
				for side := range 2 {
					go func() {
						defer close(done[side])
						movePoolItems(fresh.slice(side*4, side*4+4), old.slice(side*4, side*4+4))
					}()
				}
				for _, ch := range reached {
					select {
					case <-ch:
					case <-time.After(time.Second):
						t.Fatal("pool move did not reach boundary operation")
					}
				}
				for i := range resume {
					released[i].Do(func() { close(resume[i]) })
				}
				for _, ch := range done {
					select {
					case <-ch:
					case <-time.After(time.Second):
						t.Fatal("pool move did not complete")
					}
				}
				prev := &ends[0]
				for i := 0; i < 8; i++ {
					item := fresh.at(i)
					if prev.DirectNext() != &item.ListHead || item.ListHead.DirectPrev() != prev ||
						item.Key() != IntKey(i) || item.Value() != i || !old.at(i).IsIgnored() {
						t.Fatalf("wrong links, payload or retirement at slot %d", i)
					}
					if elist_head.IsMoved(&old.at(i).ListHead) {
						t.Fatal("pool move registered a global move")
					}
					prev = &item.ListHead
				}
				if prev.DirectNext() != &ends[1] || ends[1].DirectPrev() != prev {
					t.Fatal("wrong tail links")
				}
				runtime.KeepAlive(old)
				runtime.KeepAlive(fresh)
			})
		}
	}

}

func TestPoolInsertRetriesOldCandidate(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		for _, operation := range []string{"Get", "Delete"} {
			t.Run(fmt.Sprintf("%s/reuse=%t", operation, reuse), func(t *testing.T) {
				m := New[poolLookupKey, int](UseEmbeddedPool[poolLookupKey, int](true), MaxPefBucket[poolLookupKey, int](512))
				for i := uint64(2); i <= 16; i += 2 {
					if !m.Set(poolLookupKey(i<<40), int(i)) {
						t.Fatal("initial Set")
					}
				}
				key := poolLookupKey(12 << 40)
				pool := m.findBucket(uint64(key)).toBase().itemPool()
				old := *pool.ptrItems()
				candidate := m.bsearchBybucket(m.findBucket(uint64(key)), uint64(key), true)
				if old.Len() != 8 || candidate != old.at(5) {
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
					done <- ok && value == 12
				}()
				select {
				case <-reached:
				case <-time.After(time.Second):
					t.Fatal("lookup did not reach candidate")
				}
				if !m.Set(poolLookupKey(9<<40), 9) {
					t.Fatal("middle insertion")
				}
				if !candidate.IsIgnored() || candidate.ListHead.IsMarked() {
					t.Fatal("expected retired interior with untouched links")
				}
				if reuse {
					for offset := uint64(1); offset <= 2; offset++ {
						if !m.Set(poolLookupKey(8<<40|offset), int(offset)) {
							t.Fatal("reuse source hole")
						}
					}
					if got := m.bsearchBybucket(m.findBucket(8<<40|2), 8<<40|2, true); got != candidate {
						t.Fatal("old candidate was not reused")
					}
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
}

func TestPoolInsertKeepsInteriorLinks(t *testing.T) {
	for _, capacity := range []int{12, 13} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			p, ends := makeInsertPool(8, capacity)
			old := *p.ptrItems()
			interior := old.at(5)
			links := interior.ListHead
			reached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var release sync.Once
			elist_head.SetStepHook(func(point string, a, _, _ *elist_head.ListHead) {
				if point == "del.marked" && a == &old.at(4).ListHead {
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
				p.insertToPool(9, nil, nil)
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
			if elist_head.IsMoved(&old.at(4).ListHead) {
				t.Fatal("pool block replacement registered a global move")
			}
			if interior.Key() != 5 || interior.Value() != 5 {
				t.Fatal("reader lost its old payload during replacement")
			}
			release.Do(func() { close(resume) })
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("replacement did not finish")
			}
			if interior.ListHead != links || p.ptrItems().at(1).ListHead != links {
				t.Fatal("interior links were rewritten instead of copied unchanged")
			}
			if !interior.IsDeleted() || p.ptrItems().at(1).IsDeleted() {
				t.Fatal("retirement did not distinguish old and current entries")
			}
			runtime.KeepAlive(ends)
			runtime.KeepAlive(old)
		})
	}
}
