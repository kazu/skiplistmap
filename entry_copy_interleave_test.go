//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

func TestEntryCopyAdjacentInsert(t *testing.T) {
	m, e := newEntryCopyMap(t)
	hash, _ := e.KeyHash()
	reverse := bits.Reverse64(hash)
	var key string
	best := ^uint64(0)
	for i := 0; i < 4096; i++ {
		candidate := fmt.Sprintf("neighbor-%d", i)
		hash, _ := skiplistmap.KeyToHash(candidate)
		r := bits.Reverse64(hash)
		if reverse < r && r < best {
			key, best = candidate, r
		}
	}
	if key == "" {
		t.Fatal("no successor key")
	}
	other := skiplistmap.NewEntryMap(key, 7)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	var held atomic.Bool
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		if point == "update.found" && a == unsafe.Pointer(e.PtrListHead()) && held.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	defer skiplistmap.SetStepHook(nil)
	go func() { done <- m.Set("value", 1) }()
	<-entered
	stored := m.StoreItem(other)
	adjacent := e.PtrListHead().DirectNext() == other.PtrListHead()
	close(resume)
	updated := <-done
	if !stored || !adjacent || !updated {
		t.Fatalf("fixture: stored=%v adjacent=%v updated=%v", stored, adjacent, updated)
	}
	if got, ok := m.Get(key); !ok || got != 7 {
		t.Fatalf("adjacent key lost: Get = %v, %v", got, ok)
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(other)
}

func TestEntryCopyBetweenLinkCASes(t *testing.T) {
	testEntryCopyPausedRead(t, "replaceNode.cas2")
}

func TestEntryCopyMarkedReadRetries(t *testing.T) {
	testEntryCopyPausedRead(t, "replaceNode.marked")
}

func testEntryCopyPausedRead(t *testing.T, pause string) {
	m, e := newEntryCopyMap(t)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	var held atomic.Bool
	elist_head.SetStepHook(func(point string, a, b, c *elist_head.ListHead) {
		if point == pause && a == e.PtrListHead() && held.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	defer elist_head.SetStepHook(nil)
	go func() { done <- m.Set("value", 1) }()
	<-entered
	if !e.PtrListHead().IsMarked() {
		t.Error("old links were not marked")
	}
	retried, readDone := make(chan struct{}), make(chan struct{})
	var sawRetry atomic.Bool
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		if point == "search.marked" && sawRetry.CompareAndSwap(false, true) {
			close(retried)
		}
	})
	defer skiplistmap.SetStepHook(nil)
	go func() {
		defer close(readDone)
		if got, ok := m.Get("value"); !ok || got != 1 {
			t.Errorf("Get after retry = %v,%v", got, ok)
		}
	}()
	if pause == "replaceNode.marked" {
		<-retried
	} else {
		// Forward lookup may already reach fresh; backward lookup may
		// still see marked old. Both must return the new value, or retry.
		select {
		case <-retried:
		case <-readDone:
		}
	}
	close(resume)
	if !<-done {
		t.Fatal("Set failed")
	}
	<-readDone
	count := 0
	m.RangeItem(func(item skiplistmap.MapItem) bool { count++; return true })
	if count != 1 || m.Len() != 1 {
		t.Errorf("Range count=%d Len=%d", count, m.Len())
	}
	if !m.Purge("value") || m.Len() != 0 {
		t.Fatal("Purge after replacement failed")
	}
	runtime.KeepAlive(e)
}

func TestEntryCopyDeleteFoundOldCopy(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(fmt.Sprint(purge), func(t *testing.T) {
			m, e := newEntryCopyMap(t)
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
			var held atomic.Bool
			skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
				if point == "delete.found" && a == unsafe.Pointer(e.PtrListHead()) && held.CompareAndSwap(false, true) {
					close(entered)
					<-resume
				}
			})
			defer skiplistmap.SetStepHook(nil)
			go func() {
				if purge {
					done <- m.Purge("value")
				} else {
					done <- m.Delete("value")
				}
			}()
			<-entered
			if !m.Set("value", 1) {
				t.Error("Set failed")
			}
			close(resume)
			if !<-done {
				t.Fatal("delete failed")
			}
			if got, ok := m.Get("value"); ok || m.Len() != 0 {
				t.Fatalf("after delete Get=%v,%v Len=%d", got, ok, m.Len())
			}
			runtime.KeepAlive(e)
		})
	}
}

func TestEntryCopyConcurrentDelete(t *testing.T) {
	t.Run("Delete", func(t *testing.T) { testEntryCopyRemoval(t, false) })
	t.Run("Purge", func(t *testing.T) { testEntryCopyRemoval(t, true) })
}

func testEntryCopyRemoval(t *testing.T, purge bool) {
	m, e := newEntryCopyMap(t)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	var held atomic.Bool
	skiplistmap.SetStepHook(func(point string, a, b unsafe.Pointer) {
		if point == "update.found" && a == unsafe.Pointer(e.PtrListHead()) && held.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	defer skiplistmap.SetStepHook(nil)
	go func() { done <- m.Set("value", 1) }()
	<-entered
	var deleted bool
	if purge {
		deleted = m.Purge("value")
	} else {
		deleted = m.Delete("value")
	}
	close(resume)
	updated := <-done
	if !deleted || !updated {
		t.Fatalf("Delete=%v Set=%v", deleted, updated)
	}
	_, present := m.Get("value")
	want := 0
	if present {
		want = 1
	}
	if got := m.Len(); got != want {
		t.Fatalf("Get present=%v, Len=%v; want Len=%v", present, got, want)
	}
	runtime.KeepAlive(e)
}
