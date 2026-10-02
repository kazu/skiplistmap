//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"testing"
	"time"
	"unsafe"
)

type copySearchKey struct {
	reverse uint64
	id      int
}

func (k copySearchKey) KeyHash() (uint64, uint64)      { return bits.Reverse64(k.reverse), 1 }
func (k copySearchKey) Equal(other copySearchKey) bool { return k == other }

func TestEntryCopySearchPrefersEarlierCopy(t *testing.T) {
	for _, forward := range []bool{true, false} {
		for _, collision := range []bool{false, true} {
			if !t.Run(fmt.Sprintf("forward=%t/collision=%t", forward, collision), func(t *testing.T) {
				m := New[copySearchKey, int](BucketMode[copySearchKey, int](CombineSearch4), MaxPefBucket[copySearchKey, int](16))
				keys := make([]copySearchKey, 64)
				for i := range keys {
					keys[i] = copySearchKey{uint64(i+1) << 56, i}
					if !m.Set(keys[i], 1) {
						t.Fatal("initial Set")
					}
					if collision && !m.Set(copySearchKey{keys[i].reverse, i + 1000}, 3) {
						t.Fatal("collision Set")
					}
				}
				var key copySearchKey
				found := false
				for _, k := range keys {
					hash, _ := k.KeyHash()
					b, reverse, _ := m.searchBucket4Key4(hash, true)
					if (b.reverse < reverse) == forward {
						key, found = k, true
						break
					}
				}
				if !found {
					t.Fatal("fixture does not exercise requested search direction")
				}
				reached, release, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
				SetStepHook(func(point string, a, _ unsafe.Pointer) {
					if point == "copy.replacement.inserted" && StepEntryReverse(a) == key.reverse {
						close(reached)
						<-release
					}
				})
				t.Cleanup(func() {
					defer SetStepHook(nil)
					close(release)
					select {
					case ok := <-done:
						if !ok {
							t.Error("update failed")
						}
					case <-time.After(time.Second):
						t.Error("update did not finish")
						return
					}
					if got, ok := m.Get(key); !ok || got != 2 {
						t.Errorf("Get after update = %d,%t; want 2,true", got, ok)
					}
				})
				go func() { done <- m.Set(key, 2) }()
				select {
				case <-reached:
				case <-time.After(time.Second):
					t.Fatal("update did not reach insertion")
				}
				if got, ok := m.Get(key); !ok || got != 2 {
					t.Errorf("Get during insertion = %d,%t; want 2,true", got, ok)
				}
				if item, ok := m.LoadItem(key); !ok || item.Value() != 2 {
					t.Error("LoadItem during insertion did not select the new copy")
				}
				hash, conflict := key.KeyHash()
				if !collision {
					if got, ok := m.GetByHash(hash, conflict); !ok || got != 2 {
						t.Errorf("GetByHash during insertion = %d,%t; want 2,true", got, ok)
					}
				} else if got, ok := m.Get(copySearchKey{key.reverse, key.id + 1000}); !ok || got != 3 {
					t.Errorf("colliding key = %d,%t; want 3,true", got, ok)
				}
			}) {
				return
			}
		}
	}
}
