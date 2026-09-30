package rmap_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
)

func TestUpdateBeforePromotion(t *testing.T) {
	m := rmap.New()
	m.Set("key", 1)
	m.Set("key", 2)
	if got, ok := m.Get("key"); !ok || got != 2 {
		t.Fatalf("Get = (%v, %v), want (2, true)", got, ok)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

func TestConcurrentTransitions(t *testing.T) {
	m := rmap.New()
	const workers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprintf("worker-%d", worker)
			<-start
			for round := 0; round < 100; round++ {
				if !m.Set(key, round) {
					t.Errorf("worker %d round %d: Set failed", worker, round)
					return
				}
				if got, ok := m.Get(key); !ok || got != round {
					t.Errorf("worker %d round %d: Get = (%v, %v)", worker, round, got, ok)
					return
				}
				if !m.Delete(key) {
					t.Errorf("worker %d round %d: Delete failed", worker, round)
					return
				}
				if got, ok := m.Get(key); ok {
					t.Errorf("worker %d round %d: deleted value found: %v", worker, round, got)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	if got := m.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

func TestConcurrentInsertSameKey(t *testing.T) {
	m := rmap.New()
	const workers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			m.Set("shared", 42)
		}()
	}
	close(start)
	wg.Wait()
	if got, ok := m.Get("shared"); !ok || got != 42 {
		t.Fatalf("Get = (%v, %v), want (42, true)", got, ok)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}
