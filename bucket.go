package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap/atomic_util"
	"github.com/lk4d4/trylock"
)

const (
	bucketStateNone   uint32 = 0
	bucketStateInit   uint32 = 1
	bucketStateActive uint32 = 2
)

type bucket struct {
	_level  int32
	_len    int32
	reverse uint64
	dummy   entryHMap

	downLevels        []bucket
	cntOfActiveLevels int32
	state             uint32

	_itemPool     *samepleItemPool
	_parent       *bucket
	itemPoolFn    func() *samepleItemPool
	setItemPoolFn func(*samepleItemPool)
	headPool      list_head.ListHead
	tailPool      list_head.ListHead

	muPool trylock.Mutex // FIXME: change sync.Mutex in go 1.18

	// FIXME: debug only. should delete
	//initStart time.Time

	onOkFn func()

	LevelHead list_head.ListHead // to same level bucket
	list_head.ListHead
}

const bucketOffset = unsafe.Offsetof(emptyBucket.ListHead)
const bucketOffsetLevel = unsafe.Offsetof(emptyBucket.LevelHead)

func newBucket() (new *bucket) {

	new = &bucket{
		_itemPool: &samepleItemPool{},
	}
	new._itemPool.Init()
	new._initItemPool(true)
	new.setupPool()
	return new
}

func (e *bucket) initItemPool() {
	e._initItemPool(false)
}

func (e *bucket) _initItemPool(force bool) {

	if force || e._itemPool == nil {
		list_head.InitAsEmpty(&e.headPool, &e.tailPool)
	}
}

func (e *bucket) setupPool() {

	if e._itemPool != nil && e._itemPool.Prev() != &e.headPool {
		e.tailPool.InsertBefore(e._itemPool.PtrListHead())
	}

}

func (e *bucket) Offset() uintptr {
	return bucketOffset
}

func (e *bucket) OffsetLevel() uintptr {
	return bucketOffsetLevel
}

func (e *bucket) PtrListHead() *list_head.ListHead {
	return &e.ListHead
}

func (e *bucket) PtrLevelHead() *list_head.ListHead {
	return &e.LevelHead
}

func (e *bucket) FromListHead(head *list_head.ListHead) list_head.List {
	return bucketFromListHead(head)
}

func bucketFromListHead(head *list_head.ListHead) *bucket {
	return (*bucket)(ElementOf(unsafe.Pointer(head), bucketOffset))
}

//go:nocheckptr
func bucketFromLevelHead(head *list_head.ListHead) *bucket {
	// if head == nil {
	// 	return nil
	// }
	// return (*bucket)(unsafe.Pointer(uintptr(unsafe.Pointer(head)) - emptyBucket.OffsetLevel()))

	return (*bucket)(ElementOf(unsafe.Pointer(head), bucketOffsetLevel))
}

func (b *bucket) len() int32 {

	if b._itemPool == nil && b.itemPoolFn == nil {
		return atomic.LoadInt32(&b._len)
	}
	return int32(len(b.itemPool().items))

}

func (b *bucket) itemPool() *samepleItemPool {

	if b._itemPool != nil && b.tailPool.Prev() != b.headPool.Prev() {
		return samepleItemPoolFromListHead(b.tailPool.Prev())
	}
	if b._parent != nil {
		return b._parent.itemPool()
	}

	if b.itemPoolFn != nil {
		return b.itemPoolFn()
	}
	// FIXME: should set max of retry
	return b.itemPool()
}

func (b *bucket) SetItemPool(pool *samepleItemPool) {
	b.setItemPool(pool)
}

func (b *bucket) setItemPool(pool *samepleItemPool) {

	if b._parent != nil {
		b._parent.setItemPool(pool)
		return
	}
	if b._itemPool != nil && b.tailPool.Prev() != b.headPool.Prev() {
		b._itemPool = pool
		return
	}

	if b.tailPool.Prev() == b.headPool.Prev() {
		pool.Init()
		b.tailPool.InsertBefore(pool.PtrListHead())
		for {
			if atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&b._itemPool)),
				unsafe.Pointer(b._itemPool),
				unsafe.Pointer(pool)) {
				break
			}
			Log(LogWarn, "retry bucket._itemPool = pool")
		}
		return
	}

	if b.setItemPoolFn != nil {
		b.setItemPoolFn(pool)
		return
	}

	pool.Init()
	b._itemPool = pool
	b.setupPool()
	return
}

func (b *bucket) nextAsB() *bucket {
	//if b.ListHead.DirectNext().Empty() {
	next := b.ListHead.Next()
	if next == next.DirectNext() {
		//if next.Empty() {
		return b
	}
	//return bucketFromListHead(b.ListHead.DirectNext())
	return bucketFromListHead(next)

}

func (b *bucket) prevAsB() *bucket {

	return b._prevAsB(true)
}

func (b *bucket) _prevAsB(isSame bool) (prevB *bucket) {

	//if b.ListHead.DirectPrev().Empty() {
	prev := b.ListHead.Prev()
	if prev == prev.DirectPrev() {
		//if prev.Empty() {
		return b
	}

	//return bucketFromListHead(b.ListHead.DirectPrev())
	prevB = bucketFromListHead(prev)
	if isSame || prevB.reverse != b.reverse {
		return
	}
	return prevB._prevAsB(isSame)

}

func (b *bucket) NextOnLevel() *bucket {

	n := b.LevelHead.Next()
	nn := n.Next()
	if n == nn {
		return b
	}
	return bucketFromLevelHead(n)
	// return bucketFromLevelHead(b.LevelHead.Next())

}

func (b *bucket) PrevOnLevel() *bucket {

	p := b.LevelHead.Prev()
	pp := p.Prev()
	if p == pp {
		return b
	}

	return bucketFromLevelHead(p)

}

func (b *bucket) NextEntry() *entryHMap {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectNext().Empty() {
		head = head.DirectNext()
	}

	if !head.Empty() {
		return entryHMapFromListHead(head)
	}

	return nil

}

func (b *bucket) PrevEntry() *entryHMap {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectPrev().Empty() {
		head = head.DirectPrev()
	}

	if !head.Empty() {
		return entryHMapFromListHead(head)
	}

	return nil

}

func (b *bucket) entry(h *Map) (e HMapEntry) {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.Empty() {
		return h.ItemFn().HmapEntryFromListHead(head)
	}
	result := b.NextEntry()
	if result == nil {
		return nil
	}
	return result

}

type commonOpt struct {
	onOk bool
}

type cOptFn func(opt *commonOpt) cOptFn

func useOnOk(t bool) cOptFn {

	return func(opt *commonOpt) cOptFn {
		prev := opt.onOk
		opt.onOk = t
		return useOnOk(prev)
	}
}
func (o *commonOpt) Option(opts ...cOptFn) (prevs []cOptFn) {

	for i := range opts {
		prevs = append(prevs, opts[i](o))
	}

	return
}

func (b *bucket) largestDown(ignoreNoPool, ignoreNoInitDummy bool) *bucket {

	if len(b.downLevels) == 0 {
		return b
	}

	for i := len(b.downLevels) - 1; i > -1; i-- {

		if b.downLevels[i].level() == 0 || b.downLevels[i].reverse == 0 {
			continue
		}
		//FIXME: should not lookup direct
		if ignoreNoPool && b.downLevels[i]._itemPool == nil {
			continue
		}
		if ignoreNoInitDummy && b.downLevels[i].state != bucketStateActive {
			continue
		}
		return b.downLevels[i].largestDown(ignoreNoPool, ignoreNoInitDummy)
	}
	return b
}

func (b *bucket) _validateItemsNear() {

	var bp, bn *bucket
	_, _ = bp, bn
	if b.itemPool().validateItems() != nil {
		bp = b.prevAsB()
		bn = b.nextAsB()

	}
	if b.prevAsB().itemPool().validateItems() != nil {
		bp = b.prevAsB().prevAsB()
		bn = b
	}
	if b.nextAsB().itemPool().validateItems() != nil {
		bp = b
		bn = b.nextAsB().nextAsB()
	}

}

func (b *bucket) GetItem(r uint64) (MapItem, *samepleItemPool, unlocker) {
	return b.itemPool().getWithFn(r, &b.muPool)
}

func (b *bucket) RunLazyUnlocker(fn unlocker) {

	fn(&b.muPool)

}

func (b *bucket) headNoWaitEmpty() *elist_head.ListHead {

	return b._head()
}

func (b *bucket) head() *elist_head.ListHead {

	if b._itemPool != nil {
		return b._head()
	}

	for i := 0; i < 100; i++ {
		head := b._head()
		if head != nil && !head.Empty() {
			return head
		}
		if i > 0 {
			Log(LogWarn, "bucket.head retry=%d", i)
		}
	}
	if IsInfo() {
		isEmptyListHead := b.ListHead.Empty()
		isEmptyLevelHead := b.LevelHead.Empty()
		_, _ = isEmptyListHead, isEmptyLevelHead
	}

	return b._head()

}

func (b *bucket) _head() *elist_head.ListHead {
	if b._parent != nil {
		return b._parent.head()
	}

	// no embedded pool
	if b._itemPool == nil {
		return &b.dummy.ListHead
	}

	if b.tailPool.Prev() == b.headPool.Prev() {
		return &b.dummy.ListHead
	}

	if b.tailPool.Prev().IsMarked() {
		return b.nextAsB().head()
	}

	return &b.dummy.ListHead
}

func (b *bucket) isRequireOnOk() bool {
	return b.state != bucketStateActive && !b.ListHead.IsSingle() && !b.LevelHead.IsSingle() && !b.dummy.IsSingle() && !b.dummy.Empty()
}

func (b *bucket) setLevel(l int32) (prev int32) {

	prev = atomic.LoadInt32(&b._level)
	atomic.StoreInt32(&b._level, l)
	return
}

func (b *bucket) level() (prev int32) {

	return atomic.LoadInt32(&b._level)
}

type bucketSlice struct {
	data unsafe.Pointer
	len  int
	cap  int
}

const bucketDownlevelsOffset = unsafe.Offsetof(emptyBucket.downLevels)
const bucketSize = unsafe.Sizeof(*emptyBucket)

func (b *bucket) ptrDownLevels() *bucketSlice {

	return (*bucketSlice)(unsafe.Add(unsafe.Pointer(b), bucketDownlevelsOffset))

}

func (list *bucketSlice) at(i int) (result *bucket) {

	return list._at(i, true)
}

func (list *bucketSlice) _at(i int, checklen bool) (result *bucket) {

	if checklen && atomic_util.LoadInt(&list.len) <= i {
		return nil
	} else if atomic_util.LoadInt(&list.cap) <= i {
		return nil
	}

	data := atomic.LoadPointer(&list.data)
	return (*bucket)(unsafe.Add(data, i*int(bucketSize)))
}

func (list *bucketSlice) Len() int {

	return atomic_util.LoadInt(&list.len)
}

func (list *bucketSlice) Cap() int {

	return atomic_util.LoadInt(&list.cap)
}
