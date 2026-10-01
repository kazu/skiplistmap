package skiplistmap

import (
	"fmt"
	"io"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

type mapState uint64

const (
	mapIsDummy mapState = 1 << iota
	mapIsDeleted
	// mapIsPoolItem marks an item that the item pool of a map handed out to
	// Set, which StoreItem refuses
	mapIsPoolItem
	// mapIsBusy marks an entry that a StoreItem, a Set, a Delete or a Purge
	// is writing, which the others refuse
	mapIsBusy
	// Adding mapKeyWriting before and after replacing a pooled key/value
	// pair makes this bit odd during publication and advances its version.
	mapKeyWriting
)

type MapHead struct {
	state    mapState
	conflict uint64
	reverse  uint64
	elist_head.ListHead
}

var EmptyMapHead *MapHead

func (mh *MapHead) KeyInHmap() uint64 {
	return bits.Reverse64(mh.reverse)
}

func (mh *MapHead) IsIgnored() bool {
	return mapState(atomic.LoadUint64((*uint64)(&mh.state)))&(mapIsDummy|mapIsDeleted) > 0
}

// claimDelete sets mapIsDeleted and hold and reports whether this call set
// them, so that of two deletes of one entry only one counts it. busy reports
// that it set nothing as another call holds mapIsBusy.
func (mh *MapHead) claimDelete(hold mapState) (won, busy bool) {
	return mh.claimLive(mapIsDeleted | hold)
}

func (mh *MapHead) claimLive(hold mapState) (won, busy bool) {
	for {
		s := atomic.LoadUint64((*uint64)(&mh.state))
		if mapState(s)&mapIsDeleted != 0 {
			return false, false
		}
		if mapState(s)&mapIsBusy != 0 {
			return false, true
		}
		if atomic.CompareAndSwapUint64((*uint64)(&mh.state), s, s|uint64(hold)) {
			return true, false
		}
	}
}

// claimBusy sets mapIsBusy and reports whether this call set it.
func (mh *MapHead) claimBusy() bool {
	for {
		s := atomic.LoadUint64((*uint64)(&mh.state))
		if mapState(s)&mapIsBusy != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64((*uint64)(&mh.state), s, s|uint64(mapIsBusy)) {
			return true
		}
	}
}

func (mh *MapHead) releaseBusy() {
	atomic.AndUint64((*uint64)(&mh.state), ^uint64(mapIsBusy))
}

func (mh *MapHead) isPoolItem() bool {
	return mapState(atomic.LoadUint64((*uint64)(&mh.state)))&mapIsPoolItem > 0
}

func (mh *MapHead) IsDummy() bool {
	return mapState(atomic.LoadUint64((*uint64)(&mh.state)))&mapIsDummy > 0
}

func (mh *MapHead) IsDeleted() bool {
	return mapState(atomic.LoadUint64((*uint64)(&mh.state)))&mapIsDeleted > 0
}

func (mh *MapHead) ConflictInHamp() uint64 {
	return mh.conflict
}

func (mh *MapHead) PtrListHead() *elist_head.ListHead {
	return &(mh.ListHead)
}

const mapheadOffset = unsafe.Offsetof(EmptyMapHead.ListHead)

func (mh *MapHead) Offset() uintptr {
	return mapheadOffset
}

func mapheadFromLListHead(l *elist_head.ListHead) *MapHead {
	if l == nil {
		return nil
	}
	links := elist_head.NewList[MapHead](mapheadOffset)
	return links.Element(l)
}

func (mh *MapHead) fromListHead(l *elist_head.ListHead) *MapHead {
	return mapheadFromLListHead(l)
}

func (c *MapHead) FromListHead(l *elist_head.ListHead) *MapHead {
	return c.fromListHead(l)
}

func (c *MapHead) NextWithNil() *MapHead {
	if c.Next() == &c.ListHead {
		return nil
	}
	if c.Next().Empty() {
		return nil
	}
	return c.fromListHead(c.Next())
}

func (c *MapHead) PrevtWithNil() *MapHead {
	if c.Prev() == &c.ListHead {
		return nil
	}
	if c.Prev().Empty() {
		return nil
	}
	return c.fromListHead(c.Prev())
}

func (mhead *MapHead) dump[K Key[K], V any](w io.Writer) {

	e := fromMapHead[K, V](mhead)

	var ekey K
	if e != nil {
		ekey = e.Key()
	}
	fmt.Fprintf(w, "  entryHMap{key: %+10v, k: 0x%16x, reverse: 0x%16x), conflict: 0x%x, cur: %p, prev: %p, next: %p}\n",
		ekey, bits.Reverse64(mhead.reverse), mhead.reverse, mhead.conflict, mhead.PtrListHead(), mhead.PtrListHead().DirectPrev(), mhead.PtrListHead().DirectNext())

}

func fromMapHead[K Key[K], V any](mhead *MapHead) *Entry[K, V] {
	return mhead.recoverEntry[K, V]()
}

func (mhead *MapHead) PtrMapHead() *MapHead { return mhead }
func (mhead *MapHead) recoverEntry[K Key[K], V any]() *Entry[K, V] {
	if mhead == nil || mhead.IsDummy() {
		return nil
	}
	return newEntryList[K, V]().Element(mhead.PtrListHead())
}
