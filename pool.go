package skiplistmap

import (
	"context"
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
)

type poolCmd uint8

const (
	CmdNone poolCmd = iota
	CmdGet
	CmdPut
	CmdClose
)
const cntOfPoolMgr = 8

type successFn func(MapItem)

type poolReq struct {
	cmd       poolCmd
	item      MapItem
	onSuccess successFn
}

type Pool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	itemPool [cntOfPoolMgr]samepleItemPool
	mgrCh    [cntOfPoolMgr]chan poolReq
}

func newPool() (p *Pool) {
	p = &Pool{}
	for i := range p.mgrCh {
		p.mgrCh[i] = make(chan poolReq)
		p.itemPool[i].InitAsEmpty()
		s := &samepleItemPool{}
		s.Init()
		p.itemPool[i].DirectNext().InsertBefore(&s.ListHead)

	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return
}

func (p *Pool) startMgr() {

	for i := range p.itemPool {

		cctx, ccancel := context.WithCancel(p.ctx)
		go idxMaagement(cctx, ccancel, samepleItemPoolFromListHead(&p.itemPool[i].ListHead), p.mgrCh[i])

	}

}

func (p *Pool) Get(reverse uint64, fn successFn) {

	idx := (reverse >> (4 * 15) % cntOfPoolMgr)
	p.mgrCh[idx] <- poolReq{
		cmd:       CmdGet,
		onSuccess: fn,
	}
}

func (p *Pool) Put(item MapItem) {
	reverse := item.PtrMapHead().reverse
	idx := reverse >> (4 * 15)
	p.mgrCh[idx] <- poolReq{
		cmd:  CmdPut,
		item: item,
	}
}

// count pool per one samepleItemPool
const CntOfPersamepleItemPool = 64

type samepleItemPool struct {
	freeHead elist_head.ListHead
	freeTail elist_head.ListHead
	items    []SampleItem
	list_head.ListHead
}

var EmptysamepleItemPool *samepleItemPool = (*samepleItemPool)(unsafe.Pointer(uintptr(0)))

const samepleItemPoolOffset = unsafe.Offsetof(EmptysamepleItemPool.ListHead)

func samepleItemPoolFromListHead(head *list_head.ListHead) *samepleItemPool {
	return (*samepleItemPool)(ElementOf(unsafe.Pointer(head), samepleItemPoolOffset))
}
func (sp *samepleItemPool) Offset() uintptr {
	return samepleItemPoolOffset
}
func (sp *samepleItemPool) PtrListHead() *list_head.ListHead {
	return &(sp.ListHead)
}
func (sp *samepleItemPool) FromListHead(l *list_head.ListHead) list_head.List {
	return samepleItemPoolFromListHead(l)
}
func (sp *samepleItemPool) hasNoFree() bool {
	return sp.freeHead.DirectNext() == &sp.freeTail
}

func (sp *samepleItemPool) init() {

	elist_head.InitAsEmpty(&sp.freeHead, &sp.freeTail)

	sp.items = make([]SampleItem, 0, CntOfPersamepleItemPool)
	if !list_head.MODE_CONCURRENT {
		list_head.MODE_CONCURRENT = true
	}
	//sp.Init()
}

func (sp *samepleItemPool) Get() (new MapItem, isExpanded bool) {
	if sp.freeHead.DirectNext() == &sp.freeHead {
		sp.init()
	}

	// found free item
	if sp.freeHead.DirectNext() != &sp.freeTail {
		nElm := sp.freeTail.Prev()
		nElm.Delete()
		if nElm != nil {
			nElm.Init()
			return SampleItemFromListHead(nElm), false
		}
	}
	// not limit pool
	if cap(sp.items) > len(sp.items) {
		sp.items = sp.items[:len(sp.items)+1]
		new := &sp.items[len(sp.items)-1]
		new.Init()

		return new, false
	}

	// found next pool
	if nsp := sp.DirectNext(); nsp.DirectNext() != nsp {
		return samepleItemPoolFromListHead(nsp).Get()
	}

	// dumping is only debug mode.
	if IsDebug() {
		isExpanded = true
	}
	nPool := sp.Expand()
	new, _ = nPool.Get()
	return new, isExpanded

}

func calcCap(len int) int {

	if len > 1024 {
		return len + len/4
	}

	for i := 0; i < 60; i++ {
		if (len >> i) == 0 {
			return intPow(2, i)
		}

	}
	return 512

}

func intPow(a, b int) (r int) {
	r = 1
	for i := 0; i < b; i++ {
		r *= a
	}
	return
}

func (sp *samepleItemPool) DumpExpandInfo(w io.Writer, outers []unsafe.Pointer, format string, args ...interface{}) {

	for _, ptr := range outers {
		cur := (*elist_head.ListHead)(ptr)
		pCur := cur.DirectPrev()
		nCur := cur.DirectNext()

		fmt.Fprintf(w, format, args...)
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
		mhead.dump(w)
		mhead = EmptyMapHead.FromListHead(pCur).(*MapHead)
		mhead.dump(w)
		mhead = EmptyMapHead.FromListHead(nCur).(*MapHead)
		mhead.dump(w)
	}

}

func (sp *samepleItemPool) Expand() *samepleItemPool {

	nPool := &samepleItemPool{}
	_ = nPool

	next := sp.Next()
	pOpts := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	e := sp.MarkForDelete()
	if e != nil {
		panic("fail mark")
	}

	elist_head.InitAsEmpty(&nPool.freeHead, &nPool.freeTail)

	nCap := 0
	if len(sp.items) < 128 {
		nCap = 256
	} else {
		nCap = calcCap(len(sp.items))
	}

	nPool.items = make([]SampleItem, 0, nCap)
	nPool.items = append(nPool.items, sp.items...)

	// for debugging
	var outers []unsafe.Pointer
	var b strings.Builder
	if IsDebug() {
		outers = elist_head.OuterPtrs(
			unsafe.Pointer(&sp.items[0]),
			unsafe.Pointer(&sp.items[len(sp.items)-1]),
			unsafe.Pointer(&nPool.items[0]),
			int(SampleItemSize),
			int(SampleItemOffsetOf))
		sp.DumpExpandInfo(&b, outers, "B:rewrite reverse=0x%x\n", &sp.items[0].reverse)
	}

	elist_head.RepaireSliceAfterCopy(
		unsafe.Pointer(&sp.items[0]),
		unsafe.Pointer(&sp.items[len(sp.items)-1]),
		unsafe.Pointer(&nPool.items[0]),
		int(SampleItemSize),
		int(SampleItemOffsetOf))

	// for debugging
	if IsDebug() {
		sp.DumpExpandInfo(&b, outers, "A:rewrite reverse=0x%x\n", &sp.items[0].reverse)
		fmt.Println(b.String())
	}

	nPool.Init()

	//FIXME: check
	next.InsertBefore(&nPool.ListHead)
	list_head.DefaultModeTraverse.Option(pOpts...)
	if ok, _ := sp.IsSafety(); ok {
		sp.Init()
	} else {
		sp.IsSafety()
		fmt.Printf("old sampleItem pool is not safety")
	}
	return nPool
}

func (sp *samepleItemPool) Put(item MapItem) {

	s, ok := item.(*SampleItem)
	if !ok {
		return
	}

	pItem := uintptr(unsafe.Pointer(s))
	pTail := uintptr(unsafe.Pointer(&sp.items[len(sp.items)-1]))

	//pHead := uintptr(unsafe.Pointer(&sp.items[0]))

	if pItem <= pTail {
		sp.freeTail.InsertBefore(&s.ListHead)
	}

	samepleItemPoolFromListHead(sp.Next()).Put(item)

}

var LastItem MapItem = nil
var IsExtended = false

func idxMaagement(ctx context.Context, cancel context.CancelFunc, h *samepleItemPool, reqCh chan poolReq) {

	for req := range reqCh {
		p := samepleItemPoolFromListHead(h.Next())
		switch req.cmd {
		case CmdGet:
			e, extend := p.Get()
			LastItem = e
			// only debug mode
			if extend {
				fmt.Printf("expeand reverse=0x%x cap=%d\n ", p.items[0].reverse, cap(p.items))
				fmt.Printf("dump: sampleItemPool.items\n%s\nend: sampleItemPool.items\n", p.dump())
				IsExtended = extend
			}
			req.onSuccess(e)
			continue
		case CmdPut:
			p.Put(req.item)
			if req.onSuccess != nil {
				req.onSuccess(nil)
			}
			continue
		case CmdClose:
			ctx.Done()
		}
	}

}

func (sp *samepleItemPool) dump() string {

	var b strings.Builder

	for i := 0; i < len(sp.items); i++ {
		mhead := EmptyMapHead.FromListHead(&sp.items[i].ListHead).(*MapHead)
		mhead.dump(&b)
	}
	return b.String()
}
