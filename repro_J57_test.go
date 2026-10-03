//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// Keys a < b < c. Map A holds the items of a, b and c, stored with
// StoreItem. The caller then stores the item of b, which is still linked in
// A, into another map B. The lookup of B misses b, and _set runs Init on the
// list node of b before add2 links it into B: Init makes b self-linked while
// a.next in A still points to b and c.prev to b. add2 then links b into the
// list of B. A is broken: its list goes from a to b and on into the list of
// B, so the walk of A from its head never reaches c, and A no longer finds c.
func Test_J57StoreItemLinkedInOtherMapBreaksIt(t *testing.T) {
	keys := adjacentKeys(3)
	items := newStepItems(keys)
	ma := newStepMap()
	mb := newStepMap()
	for i := range items {
		if !ma.base.StoreItem(&items[i]) {
			t.Fatalf("StoreItem(%q) into A failed", keys[i])
		}
	}
	assertStoredInOrder(t, ma, keys)
	if t.Failed() {
		t.FailNow()
	}

	stored := mb.base.StoreItem(&items[1])
	t.Logf("StoreItem(b) into B = %v", stored)

	if err := skiplistmap.StepCheckLists[skiplistmap.StringKey, any](ma.base); err != nil {
		t.Errorf("A after StoreItem(b) into B: %v", err)
	}
	for _, k := range keys {
		var ok bool
		runWithDeadline(t, 10*time.Second, func() { _, ok = ma.Get(k) })
		if !ok {
			t.Errorf("A: Get(%q) not found after StoreItem(b) into B", k)
		}
	}
	var got []string
	runWithDeadline(t, 10*time.Second, func() {
		ma.base.RangeItem(func(item skiplistmap.MapItem[skiplistmap.StringKey, any]) bool {
			got = append(got, string(item.Key()))
			return len(got) < 16
		})
	})
	if len(got) != len(keys) {
		t.Errorf("A: RangeItem yields %q, want %q", got, keys)
	}
	runtime.KeepAlive(items)
}
