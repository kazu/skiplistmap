//go:build go1.18
// +build go1.18

package skiplistmap

import (
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap/atomic_util"
	"github.com/lk4d4/trylock"
)

type bucket[K any, V any] struct {
	_level  int32
	_len    int32
	reverse uint64
	dummy   entryHMap[K, V]

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

	onOkFn func()

	LevelHead list_head.ListHead // to same level bucket
	list_head.ListHead
}

func bucketOffset[K any, V any]() uintptr {
	return unsafe.Offsetof(emptyBucket[K, V]().ListHead)
}
func bucketOffsetLevel[K any, V any]() uintptr {
	return unsafe.Offsetof((&bucket[K, V]{}).LevelHead)
}

func newBucket[K any, V any]() (new *bucket[K, V]) {

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

func bucketFromListHead[K any, V any](head *list_head.ListHead) *bucket[K, V] {
	return (*bucket[K, V])(ElementOf(unsafe.Pointer(head), bucketOffset[K, V]()))
}

//go:nocheckptr
func bucketFromLevelHead[K any, V any](head *list_head.ListHead) *bucket[K, V] {
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
	return int32(len(b.itemPool().items))

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

func (b *bucket[K, V]) NextEntry() *entryHMap[K, V] {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectNext().Empty() {
		head = head.DirectNext()
	}

	if !head.Empty() {
		return entryHMapFromListHead[K, V](head)
	}

	return nil

}

func (b *bucket[K, V]) PrevEntry() *entryHMap[K, V] {

	if b.head() == nil {
		return nil
	}
	head := b.head()
	if !head.DirectPrev().Empty() {
		head = head.DirectPrev()
	}

	if !head.Empty() {
		return entryHMapFromListHead[K, V](head)
	}

	return nil

}

func (b *bucket[K, V]) entry(h *Map[K, V]) (e HMapEntry) {

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

func (b *bucket[K, V]) largestDown(ignoreNoPool, ignoreNoInitDummy bool) *bucket[K, V] {

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

func (b *bucket[K, V]) GetItem(r uint64) (MapItem[K, V], *samepleItemPool[K, V], unlocker) {
	return b.itemPool().getWithFn(r, &b.muPool)
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

func bucketDownlevelsOffset[K, V any]() uintptr {

	return unsafe.Offsetof((&bucket[K, V]{}).downLevels)
}

func bucketSize[K, V any]() uintptr {

	return unsafe.Sizeof(bucket[K, V]{})

}

func (b *bucket[K, V]) ptrDownLevels() *bucketSlice[K, V] {

	return (*bucketSlice[K, V])(unsafe.Add(unsafe.Pointer(b), bucketDownlevelsOffset[K, V]()))
}

// type bucketSlice[K, V any] []bucket[K, V]

// func (list bucketSlice[K, V]) _at(i int) (result *bucket[K, V]) {
// 	return &list[i]
// }

// func (list bucketSlice[K, V]) at(i int) (result *bucket[K, V]) {
// 	return &list[i]
// }

// func (list bucketSlice[K, V]) Len() int {

// 	return atomic_util.LoadInt(&list.len)
// }

// func (list bucketSlice[K, V]) Cap() int {

// 	return atomic_util.LoadInt(&list.cap)
// }

type bucketSlice[K, V any] struct {
	data unsafe.Pointer
	len  int
	cap  int
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
