package chains

import (
	"errors"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
)

type (

	// List ... manege listNode
	List struct {
		maxPerNode int
		tInfo      typeInfo
		head       list_head.ListHead
		tail       list_head.ListHead
		headE      elist_head.ListHead
		tailE      elist_head.ListHead
	}

	typeInfo struct {
		size int
	}
)

var (
	ErrUnknown error = errors.New("unknown error")
)

// New ... Make New List struct.
func New(max int) (l *List) {
	if !list_head.MODE_CONCURRENT {
		list_head.MODE_CONCURRENT = true
	}
	l = &List{}
	l.maxPerNode = max
	if max == 0 {
		l.maxPerNode = 32
	}
	list_head.InitAsEmpty(&l.head, &l.tail)
	elist_head.InitAsEmpty(&l.headE, &l.tailE)
	return l
}

// OptList ... option of List
type OptList func(*List) OptList

// ListMax ... capacity slice of ListNode
func ListMax(max int) OptList {

	return func(l *List) OptList {
		o := l.maxPerNode
		l.maxPerNode = max
		return ListMax(o)
	}
}

// ElementSize ... set size of Elemenmt struct
func ElementSize(size int) OptList {

	return func(l *List) OptList {
		o := l.tInfo.size
		l.tInfo.size = size + int(elistSize)
		return ElementSize(o)
	}
}

// Option ... apply Option of List
func (l *List) Option(opts ...OptList) (prevs []OptList) {

	for _, opt := range opts {
		prevs = append(prevs, opt(l))
	}

	return prevs
}

func (l *List) Len() (len int) {

	for cur := l.head.Next(); cur.Next() != cur; cur = cur.Next() {
		node := listNodeFromListHead(cur)
		len += node.len()
	}
	return len
}

func (l *List) Alloc() unsafe.Pointer {
	if l.tInfo.size == 0 {
		return nil
	}

	if l.Len() == 0 {
		nNode := newNode(&l.headE, &l.tailE, l.tInfo.size, l.maxPerNode)
		nNode.Init()
		l.tail.InsertBefore(nNode.PtrListHead())
		return nNode.alloc()
	}

	for cur := l.head.Next(); !cur.Empty(); cur = cur.Next() {

		node := listNodeFromListHead(cur)

		if len(node.ePtrs) == cap(node.ePtrs) {
			continue
		}

		return node.alloc()

	}
	nNode := newNode(l.tailE.Prev(), &l.tailE, l.tInfo.size, l.maxPerNode)
	nNode.Init()
	l.tail.InsertBefore(nNode.PtrListHead())

	return nNode.alloc()
}

func (l *List) At(idx int) unsafe.Pointer {
	idxOnNode := idx

	for cur := l.head.Next(); !cur.Empty(); cur = cur.Next() {

		node := listNodeFromListHead(cur)

		if idxOnNode > len(node.ePtrs) {
			idxOnNode -= len(node.ePtrs)
			continue
		}

		return node.at(idxOnNode)

	}
	return nil
}
