package skiplistmap

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

// poolIndex returns the index of the pool list of a Pool that holds the items
// of the keys of reverse: the keys of one top 4 bits share a pool.
func poolIndex(reverse uint64) uint64 {
	return reverse >> (4 * 15) % cntOfPoolMgr
}

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

	idx := poolIndex(reverse)

	if !UseGoroutineInPool {
		for retry := 0; ; retry++ {
			if retry > 0 {
				runtime.Gosched()
			}
			head := p.itemPool[idx].Next()
			if head.Empty() {
				// the list has no pool; an expand is replacing one
				continue
			}
			p := samepleItemPoolFromListHead(head)
			stepAt("pool.get.pool", unsafe.Pointer(&p.ListHead), nil)
			e, _, mu := p.Get()
			if e == nil {
				// the pool was expanded meanwhile
				continue
			}
			fn(e, mu)
			return
		}
	}

	p.mgrCh[idx] <- poolReq{
		cmd:       CmdGet,
		onSuccess: fn,
	}
}

func (p *Pool) Put(item MapItem) {
	reverse := item.PtrMapHead().reverse
	idx := poolIndex(reverse)

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
	// expanded is set under mu once _expand has replaced the pool, so that
	// a Get that waited for mu does not expand it again
	expanded atomic.Bool
	// linking counts the writers of the pool: the items that Get handed out
	// and that are not linked yet, and the writes that hold the pool with
	// holdItem or holdPool. expanding stops more writers from counting:
	// _expand copies the items only after all of them are done
	linking   atomic.Int32
	expanding atomic.Bool
	// the Lockers that Get returns with an item, held in the pool so that
	// Get allocates nothing for them
	done     linkDone
	doneLast lastLinkDone
	list_head.ListHead
}

// linkDone is the Locker that Get returns with an item: its Unlock tells the
// pool that holds it that the item is linked.
type linkDone struct{ _ byte }

func (d *linkDone) Lock() {}

func (d *linkDone) Unlock() {
	sp := (*samepleItemPool)(unsafe.Add(unsafe.Pointer(d), -int(unsafe.Offsetof(EmptysamepleItemPool.done))))
	sp.linking.Add(-1)
}

// lastLinkDone is the linkDone of the last item, whose Unlock also unlocks
// mu of the pool, which Get took for that item.
type lastLinkDone struct{ _ byte }

func (d *lastLinkDone) Lock() {}

func (d *lastLinkDone) Unlock() {
	sp := (*samepleItemPool)(unsafe.Add(unsafe.Pointer(d), -int(unsafe.Offsetof(EmptysamepleItemPool.doneLast))))
	sp.linking.Add(-1)
	sp.mu.Unlock()
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

// expandsRunning counts the expands of the item pools of every map that run,
// and writersOfAnyPool the writers that change links next to items of a pool
// they cannot name, such as the neighbors of an item that StoreItem takes out
// of the list it was deleted from. An expand copies items only while no such
// writer runs, and such a writer starts only while no expand runs.
var expandsRunning, writersOfAnyPool atomic.Int32

// holdAllPools waits until no expand runs and counts the caller as a writer
// of any pool, until releaseAllPools.
func holdAllPools() {
	for {
		writersOfAnyPool.Add(1)
		if expandsRunning.Load() == 0 {
			return
		}
		writersOfAnyPool.Add(-1)
		for expandsRunning.Load() != 0 {
			runtime.Gosched()
		}
	}
}

func releaseAllPools() {
	writersOfAnyPool.Add(-1)
}

// holdItem finds the pool of the Pool whose array holds item, the one for
// reverse, and counts a write to item on it as countLinking does, so that
// an expand copies item only after the write. It returns nil and false when
// no pool holds item: the item is not from a pool, or an expand moved it and
// the writer looks the key up again. It returns nil and true when the pool
// of item is being expanded; the writer tries again.
func (p *Pool) holdItem(reverse uint64, item MapItem) (*samepleItemPool, bool) {
	idx := poolIndex(reverse)
	addr := uintptr(unsafe.Pointer(item.PtrListHead()))
	for cur := p.itemPool[idx].Next(); !cur.Empty(); cur = cur.Next() {
		sp := samepleItemPoolFromListHead(cur)
		items := sp.ptrItems()
		base := uintptr(atomic.LoadPointer(&items.data))
		if addr < base || addr >= base+uintptr(items.Cap())*uintptr(SampleItemSize) {
			continue
		}
		if !sp.countLinking() {
			return nil, true
		}
		return sp, false
	}
	return nil, false
}

// holdPool counts a link on the pool for reverse as Get does, for a writer
// that links an item not from the pool next to the items of the pool: an
// expand of the pool copies the items only after the link. It waits while
// the pool is being expanded. The writer lowers linking of the pool after
// the link.
func (p *Pool) holdPool(reverse uint64) *samepleItemPool {
	idx := poolIndex(reverse)
	for {
		if head := p.itemPool[idx].Next(); !head.Empty() {
			if sp := samepleItemPoolFromListHead(head); sp.countLinking() {
				return sp
			}
		}
		runtime.Gosched()
	}
}

// countLinking counts a writer of sp: an item that Get is about to hand out,
// or a write that holds sp with holdItem or holdPool. It reports false,
// without counting, when an expand of sp has started: the caller then starts
// again from the pool list.
func (sp *samepleItemPool) countLinking() bool {
	sp.linking.Add(1)
	if sp.expanding.Load() {
		sp.linking.Add(-1)
		return false
	}
	return true
}

func (sp *samepleItemPool) Get() (new MapItem, isExpanded bool, lock sync.Locker) {
	if sp.freeHead.DirectNext() == &sp.freeHead {
		sp.init()
	}

	// found free item
	if sp.freeHead.DirectNext() != &sp.freeTail {
		if !sp.countLinking() {
			return nil, false, nil
		}
		nElm := sp.freeTail.Prev()
		nElm.Delete()
		if nElm != nil {
			nElm.Init()
			return SampleItemFromListHead(nElm), false, &sp.done
		}
		sp.linking.Add(-1)
	}
	// not limit pool
	pItems := sp.ptrItems()
	var mu *trylock.Mutex
	var i int
	var new2 *SampleItem
	// read the length once: the CAS below raises it from this value, and
	// another Get may take the last item between two reads
	i = pItems.Len()
	if pItems.Cap() <= i {
		goto EXPAND
	}
	if i+1 == pItems.Cap() {
		stepAt("pool.lastSlot", unsafe.Pointer(sp), nil)
		mu = &sp.mu
		mu.Lock()
		if i+1 != pItems.Cap() {
			mu.Unlock()
			new, isExpanded, lock = sp.Get()
			return
		}
	}
	if !sp.countLinking() {
		if mu != nil {
			mu.Unlock()
		}
		return nil, false, nil
	}
	if !atomic_util.CompareAndSwapInt(&pItems.len, i, i+1) {
		Log(LogWarn, "fail to increment pItem.len=%d pItem.cap=%d i=%d", pItems.Len(), pItems.Cap(), i)
		sp.linking.Add(-1)
		// the lock of the last item taken above is this Get's own; the
		// retry takes it again
		if mu != nil {
			mu.Unlock()
		}
		new, isExpanded, lock = sp.Get()
		return
	}
	new2 = (*pItems).at(i)
	new2.Init()
	if mu != nil {
		new, isExpanded, lock = new2, false, &sp.doneLast
		return
	}
	new, isExpanded, lock = new2, false, &sp.done
	return

EXPAND:
	stepAt("pool.get.expand", unsafe.Pointer(sp), nil)

	// found next pool; the node, not the link with the mark of a delete
	if nsp := sp.Next(); !nsp.Empty() {
		stepAt("pool.get.nextPool", unsafe.Pointer(sp), unsafe.Pointer(nsp))
		return samepleItemPoolFromListHead(nsp).Get()
	}

	// dumping is only debug mode.
	if IsDebug() {
		isExpanded = true
	}
	nPool, err := sp._expand()
	if err != nil {
		// another Get expanded sp meanwhile; the caller starts again from
		// the pool list
		return nil, false, nil
	}
	new, _, lock = nPool.Get()
	return new, isExpanded, lock

}

func (sp *samepleItemPool) DumpExpandInfo(w io.Writer, outers []unsafe.Pointer, format string, args ...interface{}) {

	for _, ptr := range outers {
		cur := (*elist_head.ListHead)(ptr)
		pCur := cur.DirectPrev()
		nCur := cur.DirectNext()

		fmt.Fprintf(w, format, args...)
		mhead := EmptyMapHead.FromListHead(cur)
		mhead.dump(w)
		mhead = EmptyMapHead.FromListHead(pCur)
		mhead.dump(w)
		mhead = EmptyMapHead.FromListHead(nCur)
		mhead.dump(w)
	}

}

func (sp *samepleItemPool) _expand() (*samepleItemPool, error) {

	stepAt("pool.expand.begin", unsafe.Pointer(sp), nil)
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.expanded.Load() {
		return nil, EPoolAlreadyDeleted
	}
	// stop handing out items of sp and wait for the links of the ones
	// handed out, so that the copy holds every link to them
	sp.expanding.Store(true)
	// and for the writers that may change links next to items of any pool
	expandsRunning.Add(1)
	defer expandsRunning.Add(-1)
	stepAt("pool.expand.waitLinks", unsafe.Pointer(sp), nil)
	for sp.linking.Load() != 0 || writersOfAnyPool.Load() != 0 {
		runtime.Gosched()
	}
	stepAt("pool.expand.linked", unsafe.Pointer(sp), nil)

	olen := len(sp.items)
	empty := elist_head.ListHead{}
	if sp.items[olen-1].ListHead == empty {
		Log(LogError, "last item must not be empty")
	}

	nPool := &samepleItemPool{}
	_ = nPool
	var e error
	var next *list_head.ListHead
	a := samepleItemPool{}
	if sp.ListHead == a.ListHead {
		goto NO_DELETE
	}
	next = sp.Next()
NO_DELETE:

	elist_head.InitAsEmpty(&nPool.freeHead, &nPool.freeTail)

	nCap := PoolCap(len(sp.items))

	nPool.items = make([]SampleItem, 0, nCap)
	nPool.items = append(nPool.items, sp.items...)
	stepAt("pool.expand.copied", unsafe.Pointer(sp), unsafe.Pointer(nPool))

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
		// a writer changed links next to the items while it did not hold
		// the pool: the links moved so far stay moved and the list is
		// broken, so no Get may go on
		panic(fmt.Sprintf("skiplistmap: repair of an expanded item pool: %v", err))
	}

	// for debugging
	if IsDebug() {
		sp.DumpExpandInfo(&b, outers, "A:rewrite reverse=0x%x\n", &sp.items[0].reverse)
		fmt.Println(b.String())
	}

	nPool.Init()

	// link the new pool after sp before sp leaves the pool list, so that a
	// Get walking the list always finds a pool there
	if next != nil {
		next.InsertBefore(&nPool.ListHead)
		e = sp.MarkForDelete()
		if e != nil {
			return nil, EPoolAlreadyDeleted
		}
		stepAt("pool.expand.marked", unsafe.Pointer(sp), nil)
	}
	stepAt("pool.expand.beforeSafety", unsafe.Pointer(sp), unsafe.Pointer(nPool))
	if ok, _ := sp.IsSafety(); ok {
		sp.Init()
	} else {
		sp.IsSafety()
		Log(LogWarn, "old sampleItem pool is not safety")
	}
	sp.expanded.Store(true)
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
			LastItem = e
			// only debug mode
			if extend {
				fmt.Printf("expeand reverse=0x%x cap=%d\n ", p.items[0].reverse, cap(p.items))
				fmt.Printf("dump: sampleItemPool.items\n%s\nend: sampleItemPool.items\n", p.dump())
				IsExtended = extend
			}
			// the caller unlocks mu when e is linked, as for Pool.Get
			req.onSuccess(e, mu)
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
		mhead := EmptyMapHead.FromListHead(&sp.items[i].ListHead)
		mhead.dump(&b)
	}
	return b.String()
}
