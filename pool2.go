package skiplistmap

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
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

var UseGoroutineInPool bool = false

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
	if !UseGoroutineInPool {
		return
	}
	for i := range p.itemPool {

		cctx, ccancel := context.WithCancel(p.ctx)
		go idxMaagement(cctx, ccancel, samepleItemPoolFromListHead(&p.itemPool[i].ListHead), p.mgrCh[i])

	}

}

func (p *Pool) Get(reverse uint64, fn successFn) {

	idx := (reverse >> (4 * 15) % cntOfPoolMgr)

	if !UseGoroutineInPool {
		p := samepleItemPoolFromListHead(p.itemPool[idx].Next())
		e, _ := p.Get()
		fn(e)
		return
	}

	p.mgrCh[idx] <- poolReq{
		cmd:       CmdGet,
		onSuccess: fn,
	}
}

func (p *Pool) Put(item MapItem) {
	reverse := item.PtrMapHead().reverse
	idx := reverse >> (4 * 15)

	if !UseGoroutineInPool {
		p := samepleItemPoolFromListHead(p.itemPool[idx].Next())
		p.Put(item)
		return
	}

	p.mgrCh[idx] <- poolReq{
		cmd:  CmdPut,
		item: item,
	}
}

// count pool per one samepleItemPool
const CntOfPersamepleItemPool = 64

type samepleItemPool struct {
	mu       sync.Mutex
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
	sp._init(CntOfPersamepleItemPool)
}

func (sp *samepleItemPool) _init(cap int) {

	elist_head.InitAsEmpty(&sp.freeHead, &sp.freeTail)

	sp.items = make([]SampleItem, 0, cap)
	if !list_head.MODE_CONCURRENT {
		list_head.MODE_CONCURRENT = true
	}
	//sp.Init()
}

const (
	getEmpty      byte = 1
	getLargest         = 2
	getNoCap           = 3
	requireInsert      = 4
)

func (sp *samepleItemPool) state4get(reverse uint64, tail int) byte {

	if len(sp.items) == 0 {
		return getEmpty
	}
	if cap(sp.items) == len(sp.items) {
		return getNoCap
	}

	last := &sp.items[tail]
	if last.reverse < reverse {
		return getLargest
	}
	return requireInsert

}

func (sp *samepleItemPool) validateItems() error {
	old := -1
	empty := elist_head.ListHead{}
	for i := range sp.items {
		if i == 0 && sp.items[i].PtrListHead().Prev().Next() != sp.items[i].PtrListHead() {
			return fmt.Errorf("invalid item index i=0")
		}
		if sp.items[i].ListHead == empty {
			old = i
			continue
		}

		if i == 0 {
			continue
		}

		if i == len(sp.items)-1 {
			continue
		}
		pidx := i - 1
		if pidx == old {
			pidx--
		}
		if pidx < 0 {
			continue
		}

		if sp.items[pidx].PtrListHead().Next() != sp.items[i].PtrListHead() {
			p := sp.items[pidx].Next()
			_ = p
			return fmt.Errorf("invalid item index i=%d, %d", i, i-1)
		}

		if sp.items[i].PtrListHead().Prev() != sp.items[pidx].PtrListHead() {
			p := sp.items[i].Prev()
			_ = p
			return fmt.Errorf("invalid item index i=%d, %d", i, i-1)
		}
	}
	return nil

}

var lastgets []byte = nil

func (sp *samepleItemPool) get(reverse uint64) (new MapItem, nPool *samepleItemPool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	lastActiveIdx := -1
	//var tail *SampleItem

	// MENTION: debug only
	//sp.validateItems()

	for i := len(sp.items) - 1; i >= 0; i-- {
		if sp.items[i].IsIgnored() {
			continue
		}
		lastActiveIdx = i
		//tail = &sp.items[lastActiveIdx]
		break
	}
	defer func() {
		// MENTION: debug only
		// if nPool == nil {
		// 	sp.validateItems()
		// } else {
		// 	nPool.validateItems()
		// }
	}()
	lastgets = append(lastgets, sp.state4get(reverse, lastActiveIdx))
	switch sp.state4get(reverse, lastActiveIdx) {
	case getEmpty, getLargest:
		sp.items = sp.items[:len(sp.items)+1]
		new := &sp.items[len(sp.items)-1]
		new.Init()
		return new, nil
	case getNoCap:
		nPool, err := sp.Expand()
		if err != nil {
			Log(LogWarn, "pool.Expand() require retry")
			//panic("retry?")
		}
		new, _ = nPool.get(reverse)
		return new, nPool
	}

	// require insert
	//FIXME: should bsearch
	if IsDebug() {
		var b strings.Builder
		for i := range sp.items {
			sp.items[i].PtrMapHead().dump(&b)
		}
		Log(LogDebug, "B: itemPool.items\n%s\n", b.String())
	}
	// FIXME: should disable not IsDebug()
	//sp.validateItems()

	olen := len(sp.items)
	for i := range sp.items {
		if sp.items[i].IsIgnored() {
			continue
		}
		if sp.items[i].reverse < reverse {
			continue
		}
		if i == olen-1 {
			//fmt.Printf("invalid")
		}

		sp.items = sp.items[:olen+1]
		copy(sp.items[i+1:], sp.items[i:])
		sp.items[olen-1].ListHead = sp.items[olen].ListHead
		sp.items[olen].ListHead = elist_head.ListHead{}

		_, err := sp.items[olen-1].PtrListHead().Next().InsertBefore(sp.items[olen].PtrListHead())
		if err != nil {
			panic("fail insertion")
		}
		if i == 0 && olen > 1 {
			sp.items[1].ListHead = nextListHeadOfSampleItem()
		}
		for i := 2; i < olen; i++ {
			sp.items[i].ListHead = sp.items[1].ListHead
		}

		if IsDebug() {
			var b strings.Builder
			for i := range sp.items {
				sp.items[i].PtrMapHead().dump(&b)
			}
			fmt.Printf("A: itemPool.items\n%s\n", b.String())
		}

		if i > 0 && sp.items[i-1].PtrListHead().Next() != sp.items[i].PtrListHead() {
			panic("not connect next")
		}
		if i > 0 && sp.items[i].PtrListHead().Prev() != sp.items[i-1].PtrListHead() {
			panic("not connect next.prev")
		}
		if olen-1 > 0 && sp.items[olen-1].PtrListHead().Next() != sp.items[olen].PtrListHead() {
			panic("not connect sp.items[olen-1] -> sp.items[olen]")
		}
		if olen-1 > 0 && sp.items[olen].PtrListHead().Prev() != sp.items[olen-1].PtrListHead() {
			panic("not connect sp.items[olen-1] <- sp.items[olen]")
		}
		pOpts := elist_head.SharedTrav(list_head.WaitNoM())
		sp.items[i].MarkForDelete()
		elist_head.SharedTrav(pOpts...)
		sp.items[i].Init()

		return &sp.items[i], nil
	}
	return nil, nil
}

func nextListHeadOfSampleItem() elist_head.ListHead {

	list := elist_head.NewEmptyList()

	items := make([]SampleItem, 3)

	// for i := range items {
	// 	list.Tail().InsertBefore(items[i].PtrListHead())
	// }
	list.Tail().InsertBefore(items[2].PtrListHead())
	items[2].InsertBefore(items[1].PtrListHead())
	items[1].InsertBefore(items[0].PtrListHead())

	return items[1].ListHead
}

func (sp *samepleItemPool) findIdx(reverse uint64) (int, error) {

	for i := range sp.items {
		if sp.items[i].IsIgnored() {
			continue
		}
		if reverse <= sp.items[i].reverse {
			return i, nil
		}
	}
	return -1, ErrIdxOverflow

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
	nPool, err := sp.Expand()
	if err != nil {
		panic("already deleted")
	}
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

func (sp *samepleItemPool) Split(idx int) (nPool *samepleItemPool, err error) {

	if len(sp.items) <= idx {
		return nil, ErrIdxOverflow
	}

	nPool = &samepleItemPool{}

	nPool.items = sp.items[idx:]
	sp.items = sp.items[:idx:idx]
	nPool.Init()
	//sp.validateItems()
	//nPool.validateItems()

	if sp.PtrListHead().Next() != nil && sp.PtrListHead().Next() != sp.PtrListHead() {
		_, err = sp.PtrListHead().Next().InsertBefore(nPool.PtrListHead())
	}
	return

}

func (sp *samepleItemPool) Expand() (*samepleItemPool, error) {

	nPool := &samepleItemPool{}
	_ = nPool
	var e error
	var pOpts []list_head.TravOpt
	var next *list_head.ListHead
	a := samepleItemPool{}
	if sp.ListHead == a.ListHead {
		goto NO_DELETE
	}
	next = sp.Next()
	pOpts = list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	e = sp.MarkForDelete()
	if e != nil {
		return nil, EAlreadyDeleted
	}
NO_DELETE:

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
	// FIXME: only debug
	// prev := sp.items[0].Prev()
	// _ = prev
	// cur := prev.Next()
	// _ = cur
	// prev2 := cur.Prev()
	// _ = prev2

	// if cur.PtrListHead() != &sp.items[0].ListHead {
	// 	fmt.Println("fail create")
	// }

	err := elist_head.RepaireSliceAfterCopy(
		unsafe.Pointer(&sp.items[0]),
		unsafe.Pointer(&sp.items[len(sp.items)-1]),
		unsafe.Pointer(&nPool.items[0]),
		int(SampleItemSize),
		int(SampleItemOffsetOf))

	if err != nil {
		return nil, EFailExpand
		// FIXME: only debug
		// prev := sp.items[0].Prev()
		// _ = prev
		// cur := prev.Next()
		// _ = cur
	}

	// for debugging
	if IsDebug() {
		sp.DumpExpandInfo(&b, outers, "A:rewrite reverse=0x%x\n", &sp.items[0].reverse)
		fmt.Println(b.String())
	}
	// cur = prev.Next()
	// if cur.PtrListHead() != &nPool.items[0].ListHead {
	// 	panic("fail create")
	// }

	nPool.Init()

	//FIXME: check
	next.InsertBefore(&nPool.ListHead)
	list_head.DefaultModeTraverse.Option(pOpts...)
	if ok, _ := sp.IsSafety(); ok {
		sp.Init()
	} else {
		sp.IsSafety()
		Log(LogWarn, "old sampleItem pool is not safety")
	}
	return nPool, nil
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
