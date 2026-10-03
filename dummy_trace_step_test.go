//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"testing"

	"github.com/kazu/elist_head"
)

type dummyTraceKey struct{ reverse, id uint64 }

func (k dummyTraceKey) KeyHash() (uint64, uint64)      { return bits.Reverse64(k.reverse), 7 }
func (k dummyTraceKey) Equal(other dummyTraceKey) bool { return k == other }

func TestDeletePurgeDummyTrace(t *testing.T) {
	for _, firstReverse := range []uint64{1, 17} {
		t.Run(fmt.Sprint(firstReverse), func(t *testing.T) {
			h := new(Map[dummyTraceKey, int])
			b := new(bucket[dummyTraceKey, int])
			b.dummy.state = mapIsDummy
			var tail elist_head.ListHead
			elist_head.InitAsEmpty(&b.dummy.ListHead, &tail)
			middle := MapHead{reverse: 17, state: mapIsDummy}
			middle.ListHead.Init()
			var first, target Entry[dummyTraceKey, int]
			first.InitEntry(dummyTraceKey{firstReverse, 1}, 1)
			target.InitEntry(dummyTraceKey{17, 2}, 2)
			first.ListHead.Init()
			target.ListHead.Init()
			first.reverse, first.conflict = firstReverse, 7
			target.reverse, target.conflict = 17, 7
			for _, node := range []*elist_head.ListHead{&first.ListHead, &middle.ListHead, &target.ListHead} {
				if _, err := tail.InsertBefore(node); err != nil {
					t.Fatal(err)
				}
			}

			var saved *MapHead
			entry := h.searchCopyEntry[saveDummyTrace](b, &b.dummy, 17, true, &saved)
			if entry == nil {
				t.Fatal("fixture lookup returned no entry")
			}
			entry, retry := h.matchCopyEntryWithDummy(entry, 17, 7, target.Key(), true, &saved)
			if retry || entry != &target.embeddedEntry || saved != &middle {
				t.Fatal("lookup did not return the last dummy before its matching entry")
			}

			// The ordinary search must not write tracking state, even when
			// it crosses a dummy or matches a key beyond a hash collision.
			saved = &b.dummy
			entry = h.searchCopyEntry[noDummyTrace](b, &b.dummy, 17, true, &saved)
			entry, retry = h.matchCopyEntry(entry, 17, 7, target.Key(), true)
			if retry || entry != &target.embeddedEntry || saved != &b.dummy {
				t.Fatal("ordinary lookup changed tracking state or the matching entry")
			}
		})
	}
}
