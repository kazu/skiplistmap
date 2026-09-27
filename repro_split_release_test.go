//go:build stephook

package skiplistmap_test

import (
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Test_SplitHandsOverAnItemNoLongerLinking replays this order in a map with
// the embedded pool and at most 3 entries in a bucket:
//
//  1. The top bucket 3 holds keys whose second digits of the reversed hash
//     are 1, 2 and 9.
//  2. G1 sets ka, of second digit a, under the muPool of bucket 3. _set marks
//     the item of ka as being linked and links it. The bucket is over the
//     limit: makeBucket2 splits it at 0x38, moves 9 and ka to a new bucket b,
//     publishes b and stops at makeBucket2.recurse. Before the fix, _set
//     cleared the mark only after the split, so the item was still marked.
//  3. G2 sets a key of second digit 9 above k9: it locks the muPool of b and
//     copies the items of b into a new array.
//  4. G1 resumes. The item of ka that b holds must not be left marked.
func Test_SplitHandsOverAnItemNoLongerLinking(t *testing.T) {
	m := newEmbeddedMap(3)
	nines := regionKeys(0x3, 0x9, 2)
	keys := []string{
		regionKeys(0x3, 0x1, 1)[0],
		regionKeys(0x3, 0x2, 1)[0],
		nines[0],
		regionKeys(0x3, 0xa, 1)[0],
		nines[1],
	}
	for _, k := range keys[:3] {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}

	s := newStepper(t)
	recurse := s.stopAt("map.makeBucket2.recurse", nil)
	done1 := goStep(t, func() { m.Set(keys[3], &list_head.ListHead{}) })
	recurse.waitReached(t, done1)
	done2 := goStep(t, func() { m.Set(keys[4], &list_head.ListHead{}) })
	waitDone(t, done2, "G2")
	if s.total("map.insertToPool.publish") != 1 {
		t.Fatalf("G2 did not copy the items of b")
	}
	recurse.Release()
	waitDone(t, done1, "G1")

	if skiplistmap.StepIsLinking(m.base, keys[3]) {
		t.Errorf("the item of ka is still marked as being linked after Set(ka) returned")
	}
	for _, k := range keys {
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%q) not found", k)
		}
	}
}
