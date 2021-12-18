package skiplistmap

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
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

func lazyUnlock(mu *sync.Mutex) {
	if mu != nil {
		mu.Unlock()
	}
}

type unlocker func(mu *sync.Mutex)

func (sp *samepleItemPool) appendLast(mu *sync.Mutex) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {

	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	sp.items = sp.items[:olen+1]
	new := &sp.items[olen]
	new.Init()
	return new, nil, fn

}

// requireInsert

func (sp *samepleItemPool) insertToPool(reverse uint64, mu *sync.Mutex) (newItem MapItem, nPool *samepleItemPool, fn unlocker) {
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	ocap := cap(sp.items)

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
	if olen != len(sp.items) {
		Log(LogDebug, "update olen")
		return sp.getWithFn(reverse, nil)
	}
	//olen = len(sp.items)
	nlen := int64(olen)

	for i := 0; i < olen; i++ {
		//for i := range sp.items {
		if sp.items[i].IsIgnored() {
			continue
		}
		if sp.items[i].reverse < reverse {
			continue
		}
		if i == olen-1 {
			//fmt.Printf("invalid")
		}
		if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
			Log(LogDebug, "update olen")
			return sp.getWithFn(reverse, nil)
		}
		var err error
		prevItem := sp.items[0].ListHead.Prev()
		nextItem := sp.items[olen-1].ListHead.Next()

		// copy to new slice
		newItems := make([]SampleItem, olen+1, maxInts(ocap, olen+1))
		if i > 0 {
			copy(newItems[0:i], sp.items[0:i])
		}
		copy(newItems[i+1:], sp.items[i:])

		newListHead := &elist_head.ListHead{}
		newListTail := &elist_head.ListHead{}
		elist_head.InitAsEmpty(newListHead, newListTail)

		middle := nextListHeadOfSampleItem()
		for i := 0; i < olen+1; i++ {
			newItems[i].ListHead = middle
		}

		err = newListHead.ReplaceNext(&newItems[0].ListHead, &newItems[olen].ListHead, newListTail)
		if err != nil {
			Log(LogFatal, "replace fail")
		}
		first := EmptyMapHead.fromListHead(newListHead.Next())
		last := EmptyMapHead.fromListHead(newListTail.Prev())
		pOpts := elist_head.SharedTrav(list_head.WaitNoM())
		newItems[i].MarkForDelete()
		elist_head.SharedTrav(pOpts...)
		newItems[i].Init()

		// for debug
		first = EmptyMapHead.fromListHead(newListHead.Next())
		last = EmptyMapHead.fromListHead(newListTail.Next())
		_, _ = first, last

		err = prevItem.ReplaceNext(newListHead.Next(), newListTail.Prev(), nextItem)
		if err != nil {
			Log(LogFatal, "fail to replace newItems")
		}
		oldItems := sp.items
		sp.items = newItems

		// for debug
		oldItemFirst := oldItems[0].Prev().Next().PtrMapHead()
		oldItemNext := oldItems[olen-1].Next().PtrMapHead()
		_ = oldItemFirst
		_ = oldItemNext
		ItemNext := sp.items[olen].Next().PtrMapHead()
		_ = ItemNext

		if IsDebug() {
			var b strings.Builder
			for i := range sp.items {
				sp.items[i].PtrMapHead().dump(&b)
			}
			fmt.Printf("A: itemPool.items\n%s\n", b.String())
		}

		outside := sp.items[olen].Next()
		_ = outside
		if olen != i && olen-1 != i && olen-1 > 0 && sp.items[olen-1].PtrListHead().Next() != sp.items[olen].PtrListHead() {
			toNext := sp.items[olen-1].PtrListHead().Next()
			next := sp.items[olen].PtrListHead()
			Log(LogFatal, "not connect sp.items[olen-1]=%p -> sp.items[olen]=%p ", toNext, next)
		}
		if olen != i && olen-1 != i && olen-1 > 0 && sp.items[olen].PtrListHead().Prev() != sp.items[olen-1].PtrListHead() {
			c := sp.items[olen].PtrListHead().Prev()
			p := sp.items[olen-1].PtrListHead()
			Log(LogFatal, "not connect sp.items[olen-1]=%p <- sp.items[olen]=%p", p, c)
		}

		return &sp.items[i], nil, lazyUnlock
	}
	return sp.getWithFn(reverse, nil)
	//return nil, nil, nil

}
func (sp *samepleItemPool) getWithFn(reverse uint64, mu *sync.Mutex) (new MapItem, nPool *samepleItemPool, fn unlocker) {

	lastActiveIdx := -1
	olen := len(sp.items)

	for i := olen - 1; i >= 0; i-- {
		if sp.items[i].IsIgnored() {
			continue
		}
		lastActiveIdx = i
		break
	}

	defer func() {
		if new == nil {
			Log(LogWarn, "getWithFn(): item is nil")
		}

	}()

	// for debug
	//lastgets = append(lastgets, sp.state4get(reverse, lastActiveIdx))
	switch sp.state4get(reverse, lastActiveIdx) {
	case getEmpty, getLargest:
		return sp.appendLast(mu)
	case getNoCap:
		fn, err := sp.expand(mu)
		if err != nil {
			Log(LogWarn, "pool.expand() require retry")
		}
		new, nPool, _ = sp.getWithFn(reverse, nil)
		return new, nPool, fn
	}

	return sp.insertToPool(reverse, mu)

}

func nextListHeadOfSampleItem() elist_head.ListHead {

	list := elist_head.NewEmptyList()

	items := make([]SampleItem, 3)

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
	nPool, err := sp._expand()
	if err != nil {
		panic("already deleted")
	}
	new, _ = nPool.Get()
	return new, isExpanded

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

func (sp *samepleItemPool) split(idx int) (nPool *samepleItemPool, err error) {
	return sp._split(idx, true)

}
func (sp *samepleItemPool) _split(idx int, connect bool) (nPool *samepleItemPool, err error) {

	nlen := int64(len(sp.items))
	if int(nlen) <= idx {
		return nil, ErrIdxOverflow
	}

	nPool = &samepleItemPool{}

	if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
		return sp._split(idx, connect)
	}
	nPool.items = sp.items[idx:]
	sp.items = sp.items[:idx:idx]
	nPool.Init()
	//sp.validateItems()
	//nPool.validateItems()

	if connect && sp.PtrListHead().Next() != nil && sp.PtrListHead().Next() != sp.PtrListHead() {
		_, err = sp.PtrListHead().Next().InsertBefore(nPool.PtrListHead())
	}
	return

}

func (sp *samepleItemPool) expand(mu *sync.Mutex) (unlocker, error) {
	var fn unlocker
	if mu != nil {
		mu.Lock()
		fn = lazyUnlock
	}

	olen := len(sp.items)
	//ocap := cap(sp.items)

	if olen != len(sp.items) {
		Log(LogDebug, "update olen")
		return sp.expand(nil)
	}
	nlen := int64(olen)

	if !atomic.CompareAndSwapInt64(&nlen, int64(len(sp.items)), nlen+1) {
		Log(LogDebug, "update olen")
		sp.mu.Unlock()
		return sp.expand(nil)
	}

	var err error
	prevItem := sp.items[0].ListHead.Prev()
	nextItem := sp.items[olen-1].ListHead.Next()

	nCap := 0
	if len(sp.items) < 128 {
		nCap = 256
	} else {
		nCap = calcCap(len(sp.items))
	}

	newItems := make([]SampleItem, olen, nCap)
	copy(newItems, sp.items[0:olen])

	newListHead := &elist_head.ListHead{}
	newListTail := &elist_head.ListHead{}
	elist_head.InitAsEmpty(newListHead, newListTail)

	err = newListHead.ReplaceNext(&newItems[0].ListHead, &newItems[olen-1].ListHead, newListTail)
	if err != nil {
		Log(LogFatal, "replace fail")
	}

	err = prevItem.ReplaceNext(newListHead.Next(), newListTail.Prev(), nextItem)
	if err != nil {
		Log(LogFatal, "fail to replace newItems")
	}
	oldItems := sp.items
	sp.items = newItems

	// for debug
	oldItemFirst := oldItems[0].Prev().Next().PtrMapHead()
	oldItemNext := oldItems[olen-1].Next().PtrMapHead()
	_ = oldItemFirst
	_ = oldItemNext
	ItemNext := sp.items[olen-1].Next().PtrMapHead()
	_ = ItemNext

	if IsDebug() {
		var b strings.Builder
		for i := range sp.items {
			sp.items[i].PtrMapHead().dump(&b)
		}
		fmt.Printf("A: itemPool.items\n%s\n", b.String())
	}

	return fn, nil

}

func (sp *samepleItemPool) _expand() (*samepleItemPool, error) {

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

	err := elist_head.RepaireSliceAfterCopy(
		unsafe.Pointer(&sp.items[0]),
		unsafe.Pointer(&sp.items[len(sp.items)-1]),
		unsafe.Pointer(&nPool.items[0]),
		int(SampleItemSize),
		int(SampleItemOffsetOf))

	if err != nil {
		return nil, EFailExpand
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
