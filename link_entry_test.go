package skiplistmap

import (
	"fmt"
	"testing"
	"time"

	"github.com/kazu/elist_head"
)

// A walk of linkEntry can be in a block of entries when a slide of the pool
// takes that block out of the list: the walk then ends at the last node of
// the block, not at the tail, and finds no position. linkEntry used to take
// that for the end of the list and try to append the node after the last
// entry forever. It has to start the walk again from a node that stays on
// the list, the anchor. The first node of the block is not marked, the last
// one is: both are starts that a walk can be left with.
func Test_LinkEntryRestartsFromTheAnchorWhenItsStartLeftTheList(t *testing.T) {
	for _, from := range []string{"first", "last"} {
		t.Run(from, func(t *testing.T) {
			h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true))
			for i := 0; i < 8; i++ {
				h.Set(StringKey(fmt.Sprintf("k%d", i)), i)
			}
			var live []*elist_head.ListHead
			for cur := h.head.Next(); cur != cur.Next(); cur = cur.Next() {
				if !mapheadFromLListHead(cur).IsDummy() {
					live = append(live, cur)
				}
			}
			if len(live) < 6 {
				t.Fatalf("only %d entries on the list", len(live))
			}
			// two entries in the middle of the list, as a block
			first, last := live[2], live[3]
			// a node that goes right before the block
			nb := newBucket[StringKey, any]()
			dummy := &nb.dummy
			dummy.reverse = mapheadFromLListHead(first).reverse - 1
			dummy.PtrMapHead().state |= mapIsDummy
			dummy.Init()

			// the block leaves the list before the walk starts from one of
			// its nodes, as a slide of the pool does with the entries it moves
			block := elist_head.Block{First: first, Last: last}
			if err := block.Delete(); err != nil {
				t.Fatal(err)
			}
			start := first
			if from == "last" {
				start = last
			}
			done := make(chan bool, 1)
			go func() { done <- h.linkEntry(start, dummy.PtrMapHead(), nil, withAnchor[StringKey, any](h.head)) }()
			select {
			case linked := <-done:
				if !linked || dummy.PtrListHead().IsSingle() {
					t.Fatal("the node was not linked")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("linkEntry did not start again from the anchor")
			}
			// the node is on the list, in the order of reverse
			found := false
			prev := uint64(0)
			for cur := h.head.Next(); cur != cur.Next(); cur = cur.Next() {
				if cur == dummy.PtrListHead() {
					found = true
				}
				if reverse := mapheadFromLListHead(cur).reverse; reverse < prev {
					t.Fatalf("the list is out of order at %x after %x", reverse, prev)
				} else {
					prev = reverse
				}
			}
			if !found {
				t.Fatal("the node is not on the list from the head")
			}
		})
	}
}
