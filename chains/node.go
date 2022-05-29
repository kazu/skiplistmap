package chains

import (
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

var (
	EmptylistNode *listNode = nil
	Cnt           int       = 10
)

const (
	listNodeOffset = unsafe.Offsetof(EmptylistNode.ListHead)
	elistSize      = unsafe.Sizeof(elist_head.ListHead{})
)

type (
	listNode struct {
		sizeOfElem int
		buf        []byte
		ePtrs      []unsafe.Pointer
		head       *elist_head.ListHead
		tail       *elist_head.ListHead
		list_head.ListHead
	}
)

func listNodeFromListHead(head *list_head.ListHead) *listNode {
	return (*listNode)(skiplistmap.ElementOf(unsafe.Pointer(head), listNodeOffset))
}

func (node *listNode) len() int {
	return len(node.ePtrs)
}

func newNode(head, tail *elist_head.ListHead, eSize, cap int) *listNode {
	node := &listNode{sizeOfElem: eSize, head: head, tail: tail}
	node.sizeOfElem = eSize
	node.buf = make([]byte, 0, node.sizeOfElem*cap)
	node.ePtrs = make([]unsafe.Pointer, 0, cap)

	return node
}

func (node *listNode) alloc() unsafe.Pointer {

	idx := len(node.ePtrs)
	node.buf = node.buf[:len(node.buf)+node.sizeOfElem]
	node.ePtrs = node.ePtrs[:len(node.ePtrs)+1]
	node.ePtrs[idx] = unsafe.Pointer(node.eheadFromBuf(idx))

	node.tail.InsertBefore((*elist_head.ListHead)(node.ePtrs[idx]))

	return unsafe.Add(node.ePtrs[idx], -(node.sizeOfElem - int(elistSize)))
}

func (node *listNode) eheadFromBuf(idx int) *elist_head.ListHead {

	bufidx := idx*node.sizeOfElem + node.sizeOfElem - int(elistSize)
	return (*elist_head.ListHead)(unsafe.Pointer(&node.buf[bufidx]))
}

func (node *listNode) offset() int {

	return node.sizeOfElem - int(elistSize)

}

func (node *listNode) PtrListHead() *list_head.ListHead {
	return &node.ListHead
}

func (node *listNode) at(idx int) unsafe.Pointer {

	return unsafe.Add(node.ePtrs[idx], -(node.sizeOfElem - int(elistSize)))

}
