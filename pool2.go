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

type successFn[K, V any] func(MapItem[K, V], sync.Locker)

type poolReq[K, V any] struct {
	cmd       poolCmd
	item      MapItem[K, V]
	onSuccess successFn[K, V]
}

type Pool[K, V any] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	itemPool [cntOfPoolMgr]samepleItemPool[K, V]
	mgrCh    [cntOfPoolMgr]chan poolReq[K, V]
}

func newPool[K, V any]() (p *Pool[K, V]) {
	p = &Pool[K, V]{}
	for i := range p.mgrCh {
		p.mgrCh[i] = make(chan poolReq[K, V])
		p.itemPool[i].InitAsEmpty()
		s := &samepleItemPool[K, V]{}
		s.Init()
		p.itemPool[i].DirectNext().InsertBefore(&s.ListHead)

	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return
}

func (p *Pool[K, V]) startMgr() {
	if !UseGoroutineInPool {
		return
	}
	for i := range p.itemPool {

		cctx, ccancel := context.WithCancel(p.ctx)
		go idxMaagement(cctx, ccancel, samepleItemPoolFromListHead[K, V](&p.itemPool[i].ListHead), p.mgrCh[i])

	}

}

func (p *Pool[K, V]) Get(reverse uint64, fn successFn[K, V]) {

	idx := (reverse >> (4 * 15) % cntOfPoolMgr)

	if !UseGoroutineInPool {
		p := samepleItemPoolFromListHead[K, V](p.itemPool[idx].Next())
		e, _, mu := p.Get()
		fn(e, mu)
		return
	}

	p.mgrCh[idx] <- poolReq[K, V]{
		cmd:       CmdGet,
		onSuccess: fn,
	}
}

func (p *Pool[K, V]) Put(item MapItem[K, V]) {
	reverse := item.PtrMapHead().reverse
	idx := reverse >> (4 * 15)

	if !UseGoroutineInPool {
		p := samepleItemPoolFromListHead[K, V](p.itemPool[idx].Next())
		p.Put(item)
		return
	}

	p.mgrCh[idx] <- poolReq[K, V]{
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

type samepleItemPool[K, V any] struct {
	mu       trylock.Mutex
	freeHead elist_head.ListHead
	freeTail elist_head.ListHead
	items    []SampleItem[K, V]
	list_head.ListHead
}

func EmptysamepleItemPool[K, V any]() *samepleItemPool[K, V] {
	return (*samepleItemPool[K, V])(unsafe.Pointer(uintptr(0)))
}

func samepleItemPoolOffset[K, V any]() uintptr {

	return unsafe.Offsetof(EmptysamepleItemPool[K, V]().ListHead)
}

func samepleItemPoolFromListHead[K, V any](head *list_head.ListHead) *samepleItemPool[K, V] {
	return (*samepleItemPool[K, V])(ElementOf(unsafe.Pointer(head), samepleItemPoolOffset[K, V]()))
}
func (sp *samepleItemPool[K, V]) Offset() uintptr {
	return samepleItemPoolOffset[K, V]()
}
func (sp *samepleItemPool[K, V]) PtrListHead() *list_head.ListHead {
	return &(sp.ListHead)
}
func (sp *samepleItemPool[K, V]) FromListHead(l *list_head.ListHead) list_head.List {
	return samepleItemPoolFromListHead[K, V](l)
}
func (sp *samepleItemPool[K, V]) hasNoFree() bool {
	return sp.freeHead.DirectNext() == &sp.freeTail
}

func (sp *samepleItemPool[K, V]) init() {
	sp._init(CntOfPersamepleItemPool)
}

func (sp *samepleItemPool[K, V]) _init(cap int) {

	elist_head.InitAsEmpty(&sp.freeHead, &sp.freeTail)

	sp.items = make([]SampleItem[K, V], 0, cap)
	if !list_head.MODE_CONCURRENT {
		list_head.MODE_CONCURRENT = true
	}
	//sp.Init()
}

func (sp *samepleItemPool[K, V]) validateItems() error {
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

func (sp *samepleItemPool[K, V]) Get() (new MapItem[K, V], isExpanded bool, lock sync.Locker) {
	if sp.freeHead.DirectNext() == &sp.freeHead {
		sp.init()
	}

	// found free item
	if sp.freeHead.DirectNext() != &sp.freeTail {
		nElm := sp.freeTail.Prev()
		nElm.Delete()
		if nElm != nil {
			nElm.Init()
			return SampleItemFromListHead[K, V](nElm), false, nil
		}
	}
	// not limit pool
	pItems := sp.ptrItems()
	var mu *trylock.Mutex
	var i int
	var new2 *SampleItem[K, V]
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
		return samepleItemPoolFromListHead[K, V](nsp).Get()
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

func (sp *samepleItemPool[K, V]) DumpExpandInfo(w io.Writer, outers []unsafe.Pointer, format string, args ...interface{}) {

	for _, ptr := range outers {
		cur := (*elist_head.ListHead)(ptr)
		pCur := cur.DirectPrev()
		nCur := cur.DirectNext()

		fmt.Fprintf(w, format, args...)
		mhead := EmptyMapHead.FromListHead(cur).(*MapHead)
		dumpMapHead[K, V](mhead, w)

		mhead = EmptyMapHead.FromListHead(pCur).(*MapHead)
		dumpMapHead[K, V](mhead, w)
		mhead = EmptyMapHead.FromListHead(nCur).(*MapHead)
		dumpMapHead[K, V](mhead, w)
	}

}

func (sp *samepleItemPool[K, V]) _expand() (*samepleItemPool[K, V], error) {

	sp.mu.Lock()
	defer sp.mu.Unlock()

	olen := len(sp.items)
	empty := elist_head.ListHead{}
	if sp.items[olen-1].ListHead == empty {
		Log(LogError, "last item must not be empty")
	}

	nPool := &samepleItemPool[K, V]{}
	_ = nPool
	var e error
	var pOpts []list_head.TravOpt
	var next *list_head.ListHead
	a := samepleItemPool[K, V]{}
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

	nCap := PoolCap(len(sp.items))

	nPool.items = make([]SampleItem[K, V], 0, nCap)
	nPool.items = append(nPool.items, sp.items...)

	// for debugging
	var outers []unsafe.Pointer
	var b strings.Builder
	if IsDebug() {
		outers = elist_head.OuterPtrs(
			unsafe.Pointer(&sp.items[0]),
			unsafe.Pointer(&sp.items[len(sp.items)-1]),
			unsafe.Pointer(&nPool.items[0]),
			int(SampleItemSize[K, V]()),
			int(SampleItemOffsetOf[K, V]()))
		sp.DumpExpandInfo(&b, outers, "B:rewrite reverse=0x%x\n", &sp.items[0].reverse)
	}

	err := elist_head.RepaireSliceAfterCopy(
		unsafe.Pointer(&sp.items[0]),
		unsafe.Pointer(&sp.items[len(sp.items)-1]),
		unsafe.Pointer(&nPool.items[0]),
		int(SampleItemSize[K, V]()),
		int(SampleItemOffsetOf[K, V]()))

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

func (sp *samepleItemPool[K, V]) Put(item MapItem[K, V]) {

	s, ok := item.(*SampleItem[K, V])
	if !ok {
		return
	}

	pItem := uintptr(unsafe.Pointer(s))
	pTail := uintptr(unsafe.Pointer(&sp.items[len(sp.items)-1]))

	//pHead := uintptr(unsafe.Pointer(&sp.items[0]))

	if pItem <= pTail {
		sp.freeTail.InsertBefore(&s.ListHead)
	}

	samepleItemPoolFromListHead[K, V](sp.Next()).Put(item)

}

var LastItem interface{} = nil
var IsExtended = false

func idxMaagement[K, V any](ctx context.Context, cancel context.CancelFunc, h *samepleItemPool[K, V], reqCh chan poolReq[K, V]) {

	for req := range reqCh {
		p := samepleItemPoolFromListHead[K, V](h.Next())
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

func (sp *samepleItemPool[K, V]) dump() string {

	var b strings.Builder

	for i := 0; i < len(sp.items); i++ {
		mhead := EmptyMapHead.FromListHead(&sp.items[i].ListHead).(*MapHead)
		dumpMapHead[K, V](mhead, &b)

	}
	return b.String()
}
