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
	"github.com/kazu/skiplistmap/atomic_util"
	"github.com/lk4d4/trylock"
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

type successFn func(MapItem, sync.Locker)

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
		e, _, mu := p.Get()
		fn(e, mu)
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

const (
	poolNone     uint32 = 0
	poolReading         = 1
	poolUpdating        = 2
)

type samepleItemPool struct {
	mu       trylock.Mutex
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

func (sp *samepleItemPool) Get() (new MapItem, isExpanded bool, lock sync.Locker) {
	if sp.freeHead.DirectNext() == &sp.freeHead {
		sp.init()
	}

	// found free item
	if sp.freeHead.DirectNext() != &sp.freeTail {
		nElm := sp.freeTail.Prev()
		nElm.Delete()
		if nElm != nil {
			nElm.Init()
			return SampleItemFromListHead(nElm), false, nil
		}
	}
	// not limit pool
	pItems := sp.ptrItems()
	var mu *trylock.Mutex
	var i int
	var new2 *SampleItem
	if pItems.Cap() <= pItems.Len() {
		goto EXPAND
	}
	i = pItems.Len()
	if i+1 == pItems.Cap() {
		mu = &sp.mu
		mu.Lock()
		if i+1 != pItems.Cap() {
			mu.Unlock()
			new, isExpanded, lock = sp.Get()
			return
		}
	}
	if !atomic_util.CompareAndSwapInt(&pItems.len, i, i+1) {
		Log(LogWarn, "fail to increment pItem.len=%d pItem.cap=%d i=%d", pItems.len, pItems.cap, i)
		if sp.mu.TryLock() {
			sp.mu.Unlock()
		}
		new, isExpanded, lock = sp.Get()
		return
	}
	new2 = (*pItems).at(i)
	new2.Init()
	if mu != nil {
		new, isExpanded, lock = new2, false, mu
		return
	}
	new, isExpanded, lock = new2, false, nil
	return

EXPAND:

	// found next pool
	if nsp := sp.DirectNext(); nsp.DirectNext() != nsp {
		return samepleItemPoolFromListHead(nsp).Get()
	}

	// dumping is only debug mode.
	if IsDebug() {
		isExpanded = true
	}
	nPool, err := sp._expand()
	if err != nil {
		panic("already deleted")
	}
	new, _, _ = nPool.Get()
	return new, isExpanded, nil

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

func (sp *samepleItemPool) _expand() (*samepleItemPool, error) {

	sp.mu.Lock()
	defer sp.mu.Unlock()

	olen := len(sp.items)
	empty := elist_head.ListHead{}
	if sp.items[olen-1].ListHead == empty {
		Log(LogError, "last item must not be empty")
	}

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
		return nil, EPoolAlreadyDeleted
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

	err := elist_head.RepaireSliceAfterCopy(
		unsafe.Pointer(&sp.items[0]),
		unsafe.Pointer(&sp.items[len(sp.items)-1]),
		unsafe.Pointer(&nPool.items[0]),
		int(SampleItemSize),
		int(SampleItemOffsetOf))

	if err != nil {
		return nil, EPoolExpandFail
	}

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
			e, extend, mu := p.Get()
			if mu != nil {
				mu.Unlock()
			}
			LastItem = e
			// only debug mode
			if extend {
				fmt.Printf("expeand reverse=0x%x cap=%d\n ", p.items[0].reverse, cap(p.items))
				fmt.Printf("dump: sampleItemPool.items\n%s\nend: sampleItemPool.items\n", p.dump())
				IsExtended = extend
			}
			req.onSuccess(e, nil)
			continue
		case CmdPut:
			p.Put(req.item)
			if req.onSuccess != nil {
				req.onSuccess(nil, nil)
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
