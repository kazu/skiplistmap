package skiplistmap

import (
	"context"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

type poolCmd uint8

const (
	CmdNone poolCmd = iota
	CmdGet
	CmdPut
	CmdClose
)
const cntOfPoolMgr = 16

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
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return
}

func (p *Pool) startMgr() {

	for i := range p.itemPool {

		cctx, ccancel := context.WithCancel(p.ctx)
		go idxMaagement(cctx, ccancel, &p.itemPool[i], p.mgrCh[i])

	}

}

func (p *Pool) Get(reverse uint64, fn successFn) {

	idx := reverse >> (4 * 15)
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
const CntOfPersamepleItemPool = 512

type samepleItemPool struct {
	freeHead *list_head.ListHead
	freeTail *list_head.ListHead
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
	return sp.freeHead.DirectNext() == sp.freeTail
}

func (sp *samepleItemPool) init() {
	list := &list_head.ListHead{}
	list.InitAsEmpty()
	sp.freeHead = list.DirectPrev()
	sp.freeTail = list.DirectNext()
	sp.items = make([]SampleItem, 0, CntOfPersamepleItemPool)
	sp.Init()
}

func (sp *samepleItemPool) Get() (new MapItem) {
	if sp.freeHead == nil {
		sp.init()
	}

	// found free item
	if sp.freeHead.DirectNext() != sp.freeTail {
		_, nElm := sp.freeTail.Prev().Purge()
		if nElm != nil {
			nElm.Init()
			return SampleItemFromListHead(nElm)
		}
	}
	// not limit pool
	if cap(sp.items) > len(sp.items) {
		sp.items = sp.items[:len(sp.items)+1]
		new := &sp.items[len(sp.items)-1]
		new.Init()

		return new
	}

	// found next pool
	if nsp := sp.DirectNext(); nsp.DirectNext() != nsp {
		return samepleItemPoolFromListHead(nsp).Get()
	}

	nPool := &samepleItemPool{}
	nPool.init()

	sp.DirectNext().InsertBefore(&nPool.ListHead)
	return nPool.Get()

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

func idxMaagement(ctx context.Context, cancel context.CancelFunc, p *samepleItemPool, reqCh chan poolReq) {

	for req := range reqCh {
		switch req.cmd {
		case CmdGet:
			e := p.Get()
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
