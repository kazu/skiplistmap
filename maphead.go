package skiplistmap

import (
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

type mapState uint32

const (
	mapIsDummy mapState = 1 << iota
	mapIsDeleted
	// mapIsLinking is held by the one store that links the entry, so that
	// another store of the same item does not Init it in the middle
	mapIsLinking
)

type MapHead struct {
	state    mapState
	conflict uint64
	reverse  uint64
	elist_head.ListHead
}

var EmptyMapHead *MapHead = (*MapHead)(unsafe.Pointer(uintptr(0)))

func (mh *MapHead) KeyInHmap() uint64 {
	return bits.Reverse64(mh.reverse)
}

func (mh *MapHead) IsIgnored() bool {
	return mapState(atomic.LoadUint32((*uint32)(&mh.state)))&(mapIsDummy|mapIsDeleted) > 0
}

// claimLink takes mapIsLinking for the calling store and reports whether it
// got it. A store that does not get it waits with waitLinked.
func (mh *MapHead) claimLink() bool {
	for {
		s := atomic.LoadUint32((*uint32)(&mh.state))
		if mapState(s)&mapIsLinking != 0 {
			return false
		}
		if atomic.CompareAndSwapUint32((*uint32)(&mh.state), s, s|uint32(mapIsLinking)) {
			return true
		}
	}
}

func (mh *MapHead) releaseLink() {
	atomic.AndUint32((*uint32)(&mh.state), ^uint32(mapIsLinking))
}

// waitLinked waits until the store holding mapIsLinking is done.
func (mh *MapHead) waitLinked() {
	for mapState(atomic.LoadUint32((*uint32)(&mh.state)))&mapIsLinking != 0 {
		runtime.Gosched()
	}
}

func (mh *MapHead) IsDummy() bool {
	return mh.state&mapIsDummy > 0
}

func (mh *MapHead) IsDeleted() bool {
	return mh.state&mapIsDeleted > 0
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

func (mhead *MapHead) dump(w io.Writer) {

	e := fromMapHead(mhead)

	var ekey interface{}
	ekey = e.Key()
	fmt.Fprintf(w, "  entryHMap{key: %+10v, k: 0x%16x, reverse: 0x%16x), conflict: 0x%x, cur: %p, prev: %p, next: %p}\n",
		ekey, bits.Reverse64(mhead.reverse), mhead.reverse, mhead.conflict, mhead.PtrListHead(), mhead.PtrListHead().DirectPrev(), mhead.PtrListHead().DirectNext())

}

func fromMapHead(mhead *MapHead) MapItem {

	if mhead.IsDummy() {
		return entryHMapFromListHead(mhead.PtrListHead())
	}
	return SampleItemFromListHead(mhead.PtrListHead())
}
