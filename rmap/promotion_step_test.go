//go:build stephook

package rmap

import (
	smap "github.com/kazu/skiplistmap"
	"sync"
	"testing"
	"time"
)

func TestLenDuringDeleteAndRevival(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("key", 1)
	m.Get("key")
	paused, resume, deleted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	stepHook = func(point string) {
		if point == "delete.beforeCount" {
			close(paused)
			<-resume
		}
	}
	go func() { m.Delete("key"); close(deleted) }()
	<-paused
	m.Set("key", 2)
	result := make(chan int, 1)
	go func() { result <- m.Len() }()
	select {
	case n := <-result:
		if n < 0 || n > 1 {
			t.Errorf("Len = %d, but the map only held zero or one key", n)
		}
		result <- n
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	<-deleted
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Error("Len did not finish after deletion completed")
	}
	stepHook = nil
	if n := m.Len(); n != 1 {
		t.Errorf("final Len = %d, want 1", n)
	}
}

func TestLenDoesNotCombineDifferentStates(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("read", 1)
	m.Get("read")
	loaded, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	dirtyLoaded, dirtyResume := make(chan struct{}), make(chan struct{})
	var once, dirtyOnce sync.Once
	stepHook = func(point string) {
		if point == "len.readCount" {
			once.Do(func() { close(loaded); <-resume })
		}
		if point == "len.dirtyCount" {
			dirtyOnce.Do(func() { close(dirtyLoaded); <-dirtyResume })
		}
	}
	var got int
	go func() { got = m.Len(); close(done) }()
	select {
	case <-loaded:
	case <-time.After(2 * time.Second):
		t.Fatal("Len did not read its first count")
	}
	m.Delete("read")
	m.Set("dirty", 2) // There has never been more than one live key.
	close(resume)
	select {
	case <-dirtyLoaded:
	case <-time.After(2 * time.Second):
		t.Fatal("Len did not read its dirty count")
	}
	m.Delete("dirty")
	m.Set("read", 3) // The read count is one again; value equality misses this ABA.
	close(dirtyResume)
	<-done
	stepHook = nil
	if got != 0 && got != 1 {
		t.Fatalf("Len = %d, but the map only held zero or one key", got)
	}
}

func TestDetachedValueDoesNotOverwriteReinsertedKey(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("first", 1)
	loaded, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, release sync.Once
	stepHook = func(point string) {
		if point == "value.loaded" {
			once.Do(func() {
				close(loaded)
				<-resume
			})
		}
	}
	go func() { m.Set("first", 2); close(done) }()
	defer func() {
		release.Do(func() { close(resume) })
		<-done
		stepHook = nil
	}()
	select {
	case <-loaded:
	case <-time.After(2 * time.Second):
		t.Fatal("update did not load its value cell")
	}
	m.Get("first")
	m.Delete("first")
	m.Set("other", 3)
	m.Get("missing") // Omit the deleted slot in another promotion.
	m.Set("first", 4)
	release.Do(func() { close(resume) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("update did not finish")
	}
	if got, ok := m.Get("first"); !ok || got != 4 {
		t.Fatalf("reinserted key = (%v, %v), want (4, true)", got, ok)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestPromotionKeepsLateInsert(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("first", 1)
	copied := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan struct{})
	stepHook = func(point string) {
		if point == "promote.copied" {
			close(copied)
			<-resume
		}
	}
	go func() {
		m.Get("missing")
		close(done)
	}()
	select {
	case <-copied:
	case <-time.After(2 * time.Second):
		t.Fatal("promotion did not reach copied step")
	}
	m.Set("late", 42)
	close(resume)
	<-done
	stepHook = nil
	if got, ok := m.Get("late"); !ok || got != 42 {
		t.Fatalf("late insert lost: Get = (%v, %v)", got, ok)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
}

func TestConcurrentInsertCountsOneKey(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("anchor", 1)
	arrived := make(chan struct{}, 2)
	resume := make(chan struct{})
	stepHook = func(point string) {
		if point == "set.dirtyMissing" {
			arrived <- struct{}{}
			<-resume
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Set("shared", 42)
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatal("insertion did not reach dirty step")
		}
	}
	close(resume)
	wg.Wait()
	stepHook = nil
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
}

func TestPromotionDoesNotBlockWriter(t *testing.T) {
	for _, point := range []string{"promote.closing", "promote.closed", "promote.copied"} {
		t.Run(point, func(t *testing.T) {
			m := New[smap.StringKey, int]()
			m.Set("first", 1)
			paused := make(chan struct{})
			resume := make(chan struct{})
			done := make(chan struct{})
			var once sync.Once
			stepHook = func(p string) {
				if p == point {
					once.Do(func() { close(paused); <-resume })
				}
			}
			go func() { m.Get("missing"); close(done) }()
			select {
			case <-paused:
			case <-time.After(2 * time.Second):
				t.Fatal("promotion did not pause")
			}
			written := make(chan struct{})
			go func() {
				m.Set("late", 2)
				m.Set("first", 3)
				m.Delete("first")
				m.Set("first", 4)
				close(written)
			}()
			select {
			case <-written:
			case <-time.After(2 * time.Second):
				close(resume)
				<-done
				<-written
				stepHook = nil
				t.Fatal("writer waited for promotion")
			}
			close(resume)
			<-done
			stepHook = nil
			for key, want := range map[string]int{"first": 4, "late": 2} {
				if got, ok := m.Get(smap.StringKey(key)); !ok || got != want {
					t.Errorf("Get(%q) = (%v, %v), want %d", key, got, ok, want)
				}
			}
			if got := m.Len(); got != 2 {
				t.Fatalf("Len = %d, want 2", got)
			}
		})
	}
}

func TestPromotionKeepsRevivedKey(t *testing.T) {
	m := New[smap.StringKey, int]()
	m.Set("key", 1)
	m.Get("key")
	m.Delete("key")
	paused := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	stepHook = func(p string) {
		if p == "promote.copied" {
			once.Do(func() { close(paused); <-resume })
		}
	}
	go func() { m.Set("other", 9); m.Get("missing"); close(done) }()
	select {
	case <-paused:
	case <-time.After(2 * time.Second):
		t.Fatal("promotion did not pause")
	}
	m.Set("key", 2)
	close(resume)
	<-done
	stepHook = nil
	if got, ok := m.Get("key"); !ok || got != 2 {
		t.Fatalf("revived key lost: (%v, %v)", got, ok)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
}
