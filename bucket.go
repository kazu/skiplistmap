package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap/atomic_util"
	"github.com/lk4d4/trylock"
)

const (
	bucketStateNone   uint32 = 0
	bucketStateInit   uint32 = 1
	bucketStateActive uint32 = 2
)

type bucket[K Key[K], V any] struct {
	_level  int32 // < 0 while a split builds the slot; lookups skip level() <= 0
	_len    int32
	reverse uint64
	dummy   MapHead

	downLevels        []bucket[K, V]
	cntOfActiveLevels int32
	state             uint32

	_itemPool     *samepleItemPool[K, V]
	_parent       *bucket[K, V]
	itemPoolFn    func() *samepleItemPool[K, V]
	setItemPoolFn func(*samepleItemPool[K, V])
	headPool      list_head.ListHead
	tailPool      list_head.ListHead

	muPool trylock.Mutex // FIXME: change sync.Mutex in go 1.18

	// FIXME: debug only. should delete
	//initStart time.Time

	onOkFn atomic.Pointer[func()]

	LevelHead list_head.ListHead // to same level bucket
	list_head.ListHead
}

func bucketOffset[K Key[K], V any]() uintptr {
	return unsafe.Offsetof(emptyBucket[K, V]().ListHead)
}
func bucketOffsetLevel[K Key[K], V any]() uintptr {
	return unsafe.Offsetof(emptyBucket[K, V]().LevelHead)
}

func newBucket[K Key[K], V any]() (new *bucket[K, V]) {

	new = &bucket[K, V]{
		_itemPool: &samepleItemPool[K, V]{},
	}
	new._itemPool.Init()
	new._initItemPool(true)
	new.setupPool()
	return new
}

func (e *bucket[K, V]) initItemPool() {
	e._initItemPool(false)
}

func (e *bucket[K, V]) _initItemPool(force bool) {

	if force || e._itemPool == nil {
		list_head.InitAsEmpty(&e.headPool, &e.tailPool)
	}
}

func (e *bucket[K, V]) setupPool() {

	if e._itemPool != nil && e._itemPool.Prev() != &e.headPool {
		e.tailPool.InsertBefore(e._itemPool.PtrListHead())
	}

}

func (e *bucket[K, V]) Offset() uintptr {
	return bucketOffset[K, V]()
}

func (e *bucket[K, V]) OffsetLevel() uintptr {
	return bucketOffsetLevel[K, V]()
}

func (e *bucket[K, V]) PtrListHead() *list_head.ListHead {
	return &e.ListHead
}

func (e *bucket[K, V]) PtrLevelHead() *list_head.ListHead {
	return &e.LevelHead
}

func (e *bucket[K, V]) FromListHead(head *list_head.ListHead) list_head.List {
	return bucketFromListHead[K, V](head)
}

func bucketFromListHead[K Key[K], V any](head *list_head.ListHead) *bucket[K, V] {
	return (*bucket[K, V])(ElementOf(unsafe.Pointer(head), bucketOffset[K, V]()))
}

//go:nocheckptr
func bucketFromLevelHead[K Key[K], V any](head *list_head.ListHead) *bucket[K, V] {
	// if head == nil {
	// 	return nil
	// }
	// return (*bucket)(unsafe.Pointer(uintptr(unsafe.Pointer(head)) - emptyBucket.OffsetLevel()))

	return (*bucket[K, V])(ElementOf(unsafe.Pointer(head), bucketOffsetLevel[K, V]()))
}

func (b *bucket[K, V]) len() int32 {

	if b._itemPool == nil && b.itemPoolFn == nil {
		return atomic.LoadInt32(&b._len)
	}
	return int32(b.itemPool().ptrItems().Len())

}

// entries is len without the free slots a split left before the entries
// of the pool, which the inserts below them take: the count a split is
// decided on.
func (b *bucket[K, V]) entries() int {
	if b._itemPool == nil && b.itemPoolFn == nil {
		return int(b.len())
	}
	return int(b.len()) - b.itemPool().leadingFree()
}

func (b *bucket[K, V]) itemPool() *samepleItemPool[K, V] {

	if b._itemPool != nil && b.tailPool.Prev() != b.headPool.Prev() {
		return samepleItemPoolFromListHead[K, V](b.tailPool.Prev())
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

func (b *bucket[K, V]) SetItemPool(pool *samepleItemPool[K, V]) {
	b.setItemPool(pool)
}

func (b *bucket[K, V]) setItemPool(pool *samepleItemPool[K, V]) {

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

func (b *bucket[K, V]) nextAsB() *bucket[K, V] {
	//if b.ListHead.DirectNext().Empty() {
	next := b.ListHead.Next()
	if next == next.DirectNext() {
		//if next.Empty() {
		return b
	}
	//return bucketFromListHead(b.ListHead.DirectNext())
	return bucketFromListHead[K, V](next)

}

func (b *bucket[K, V]) prevAsB() *bucket[K, V] {

	return b._prevAsB(true)
}

func (b *bucket[K, V]) _prevAsB(isSame bool) (prevB *bucket[K, V]) {

	//if b.ListHead.DirectPrev().Empty() {
	prev := b.ListHead.Prev()
	if prev == prev.DirectPrev() {
		//if prev.Empty() {
		return b
	}

	//return bucketFromListHead(b.ListHead.DirectPrev())
	prevB = bucketFromListHead[K, V](prev)
	if isSame || prevB.reverse != b.reverse {
		return
	}
	return prevB._prevAsB(isSame)

}

func (b *bucket[K, V]) NextOnLevel() *bucket[K, V] {

	n := b.LevelHead.Next()
	nn := n.Next()
	if n == nn {
		return b
	}
	return bucketFromLevelHead[K, V](n)
	// return bucketFromLevelHead(b.LevelHead.Next())

}

func (b *bucket[K, V]) PrevOnLevel() *bucket[K, V] {

	p := b.LevelHead.Prev()
	pp := p.Prev()
	if p == pp {
		return b
	}

	return bucketFromLevelHead[K, V](p)

}

func (b *bucket[K, V]) NextEntry() *MapHead {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectNext().Empty() {
		head = head.DirectNext()
	}

	if !head.Empty() {
		return mapheadFromLListHead(head)
	}

	return nil

}

func (b *bucket[K, V]) PrevEntry() *MapHead {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectPrev().Empty() {
		head = head.DirectPrev()
	}

	if !head.Empty() {
		return mapheadFromLListHead(head)
	}

	return nil

}

func (b *bucket[K, V]) entry(h *Map[K, V]) (e *MapHead) {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.Empty() {
		return mapheadFromLListHead(head)
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

func (b *bucket[K, V]) largestDown(ignoreNoPool, ignoreNoInitDummy bool) *bucket[K, V] {

	downs := b.ptrDownLevels()
	if downs.Cap() == 0 {
		return b
	}

	for i := downs.Len() - 1; i > -1; i-- {
		down := downs.at(i)
		if down.level() <= 0 || down.reverse == 0 {
			continue
		}
		//FIXME: should not lookup direct
		if ignoreNoPool && down._itemPool == nil {
			continue
		}
		if ignoreNoInitDummy && down.state != bucketStateActive {
			continue
		}
		return down.largestDown(ignoreNoPool, ignoreNoInitDummy)
	}
	return b
}

func (b *bucket[K, V]) _validateItemsNear() {

	var bp, bn *bucket[K, V]
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

func (b *bucket[K, V]) GetItem(r uint64) (*embeddedEntry[K, V], *samepleItemPool[K, V], unlocker) {
	return b.itemPool().getWithFn(r, &b.muPool, nil)
}

func (b *bucket[K, V]) RunLazyUnlocker(fn unlocker) {

	fn(&b.muPool)

}

func (b *bucket[K, V]) headNoWaitEmpty() *elist_head.ListHead {

	return b._head()
}

func (b *bucket[K, V]) head() *elist_head.ListHead {

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

func (b *bucket[K, V]) _head() *elist_head.ListHead {
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

func (b *bucket[K, V]) isRequireOnOk() bool {
	return b.state != bucketStateActive && !b.ListHead.IsSingle() && !b.LevelHead.IsSingle() && !b.dummy.IsSingle() && !b.dummy.Empty()
}

func (b *bucket[K, V]) setLevel(l int32) (prev int32) {

	prev = atomic.LoadInt32(&b._level)
	atomic.StoreInt32(&b._level, l)
	return
}

func (b *bucket[K, V]) level() (prev int32) {

	return atomic.LoadInt32(&b._level)
}

// childLevel returns the level of the buckets in the downLevels of b. The
// level of b is negative while its split is not finished, so it is read as
// its absolute value.
func (b *bucket[K, V]) childLevel() int32 {
	l := b.level()
	if l < 0 {
		l = -l
	}
	return l + 1
}

type bucketSlice[K Key[K], V any] struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func bucketDownlevelsOffset[K Key[K], V any]() uintptr {
	return unsafe.Offsetof(emptyBucket[K, V]().downLevels)
}
func bucketSize[K Key[K], V any]() uintptr {
	return unsafe.Sizeof(*emptyBucket[K, V]())
}

func (b *bucket[K, V]) ptrDownLevels() *bucketSlice[K, V] {

	return (*bucketSlice[K, V])(unsafe.Add(unsafe.Pointer(b), bucketDownlevelsOffset[K, V]()))

}

func (list *bucketSlice[K, V]) at(i int) (result *bucket[K, V]) {

	return list._at(i, true)
}

func (list *bucketSlice[K, V]) _at(i int, checklen bool) (result *bucket[K, V]) {

	if checklen && atomic_util.LoadInt(&list.len) <= i {
		return nil
	} else if atomic_util.LoadInt(&list.cap) <= i {
		return nil
	}

	data := atomic.LoadPointer(&list.data)
	return (*bucket[K, V])(unsafe.Add(data, i*int(bucketSize[K, V]())))
}

func (list *bucketSlice[K, V]) Len() int {

	return atomic_util.LoadInt(&list.len)
}

func (list *bucketSlice[K, V]) Cap() int {

	return atomic_util.LoadInt(&list.cap)
}
