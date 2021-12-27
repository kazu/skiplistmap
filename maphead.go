package skiplistmap

import (
	"fmt"
	"io"
	"math/bits"
	"unsafe"

	"github.com/kazu/elist_head"
)

type mapState uint32

const (
	mapIsDummy mapState = 1 << iota
	mapIsDeleted
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
	return mh.state > 0
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

//go:nocheckptr
func mapheadFromLListHead(l *elist_head.ListHead) *MapHead {
	return (*MapHead)(ElementOf(unsafe.Pointer(l), mapheadOffset))
}

func (mh *MapHead) fromListHead(l *elist_head.ListHead) *MapHead {
	return mapheadFromLListHead(l)
}

func (c *MapHead) FromListHead(l *elist_head.ListHead) elist_head.List {
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
