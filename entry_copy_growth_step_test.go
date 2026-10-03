//go:build stephook

package skiplistmap_test

import (
	"fmt"
	"math/bits"
	"testing"
	"time"
	"unsafe"

	"github.com/kazu/skiplistmap"
)

func TestEntryCopyPoolGrowth(t *testing.T) {
	for _, size := range []int{16, 32} {
		for _, point := range []string{"elist.insert.begin", "map.copy.replacement.inserted", "elist.del.nextMarked"} {
			if !t.Run(fmt.Sprintf("bucket=%d/%s", size, point), func(t *testing.T) {
				m := newHashStepMap()
				skiplistmap.MaxPefBucket[fixedHashKey, any](size)(m)
				key := func(name string, reverse uint64) fixedHashKey {
					return fixedHashKey{name, bits.Reverse64(reverse), 1}
				}
				a := key("A", 0x3000000000010000)
				b := key("B", 0x3000000000010001)
				c := key("C", 0x3000000000010002)
				for _, k := range []fixedHashKey{a, b, c} {
					if !m.Set(k, 1) {
						t.Fatal("initial Set")
					}
				}
				for i := 0; i < skiplistmap.CntOfPersamepleItemPool-3; i++ {
					if !m.Set(key(fmt.Sprint(i), 0x3000000000000010+uint64(i)), i) {
						t.Fatal("prefill Set")
					}
				}
				if !m.Set(b, 2) {
					t.Fatal("first B update")
				}
				s := newStepper(t)
				paused := s.stopAt(point, func(a, _, _ unsafe.Pointer) bool {
					return skiplistmap.StepEntryReverse(a) == 0x3000000000010001
				})
				updated := make(chan struct{})
				go func() {
					defer close(updated)
					if !m.Set(b, 3) {
						t.Error("concurrent B update")
					}
				}()
				paused.waitReached(t, updated)
				moving := s.stopAt("elist.move.marked", nil)
				grown := make(chan struct{})
				go func() {
					defer close(grown)
					if !m.Set(key("grow", 0x3000000000001000), 1) {
						t.Error("growing Set")
					}
				}()
				moving.waitReached(t, grown)
				paused.Release()
				moving.Release()
				for _, done := range []<-chan struct{}{updated, grown} {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("Set did not finish during pool growth")
					}
				}
				lastUpdate := make(chan bool, 1)
				go func() { lastUpdate <- m.Set(b, 4) }()
				select {
				case ok := <-lastUpdate:
					if !ok {
						t.Fatal("B update after pool growth")
					}
				case <-time.After(time.Second):
					t.Fatal("B update after pool growth did not finish")
				}
				for _, k := range []fixedHashKey{a, b, c} {
					want := 1
					if k == b {
						want = 4
					}
					if value, ok := m.Get(k); !ok || value != want {
						t.Fatalf("Get(%s)=%v,%v; want %d", k.name, value, ok, want)
					}
				}
				if m.Len() != skiplistmap.CntOfPersamepleItemPool+1 {
					t.Fatalf("Len=%d", m.Len())
				}
			}) {
				return
			}
		}
	}
}
