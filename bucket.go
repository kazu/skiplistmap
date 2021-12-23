package skiplistmap

import (
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/loncha"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap/atomic_util"
)

const (
	bucketStateNone   uint32 = 0
	bucketStateInit   uint32 = 1
	bucketStateActive uint32 = 2
)

type bucket struct {
	level   int32
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

	muPool sync.Mutex

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
		return b._len
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
		b._itemPool = pool
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

func (b *bucket) checklevel() error {

	level := int32(-1)
	var reverse uint64
	for cur := b.LevelHead.DirectNext(); !cur.Empty(); cur = cur.DirectNext() {
		b := bucketFromLevelHead(cur)
		if level == -1 {
			level = bucketFromLevelHead(cur).level
			reverse = b.reverse
			continue
		}
		if level != bucketFromLevelHead(cur).level {
			return errors.New("invalid level")
		}
		if reverse < b.reverse {
			return errors.New("invalid reverse")
		}
		reverse = b.reverse
	}
	level = -1
	for cur := b.LevelHead.DirectPrev(); !cur.Empty(); cur = cur.DirectPrev() {
		b := bucketFromLevelHead(cur)
		if level == -1 {
			level = bucketFromLevelHead(cur).level
			reverse = b.reverse
			continue
		}
		if level != bucketFromLevelHead(cur).level {
			return errors.New("invalid level")
		}
		if reverse > b.reverse {
			return errors.New("invalid reverse")
		}
		reverse = b.reverse
	}
	return nil

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

func (h *Map) FindBucket(reverse uint64) (b *bucket) {
	return h._findBucket(reverse, false, false)
}

func (h *Map) findBucket(reverse uint64) (b *bucket) {

	return h._findBucket(reverse, false, false)
}

func (h *Map) _findBucket(reverse uint64, ignoreNoPool bool, ignoreNoInitDummy bool) (b *bucket) {

	emptyeList := elist_head.ListHead{}

	results := []*bucket{}

	defer func() {
		if h.isEmbededItemInBucket || b._parent != nil || b.dummy.ListHead != emptyeList {
			return
		}
		results = append(results, b)
		if !h.isEmbededItemInBucket {
			loncha.Reverse(results)
		}
		for _, cBucket := range results {
			if cBucket.dummy.ListHead != emptyeList {
				b = cBucket
				return
			}
		}
		Log(LogWarn, "not found with inited-dummpy")
	}()

	for l := 1; l < 16; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			if !h.isEmbededItemInBucket {
				results = append(results, &h.buckets[idx])
			}
			b = &h.buckets[idx]
			continue
		}
		var bucketDowns *bucketSlice
		bucketDowns = b.ptrDownLevels()
		if bucketDowns == nil || atomic_util.LoadInt(&bucketDowns.cap) == 0 {
			break
		}

		for {
			if atomic_util.LoadInt(&bucketDowns.len) > 0 {
				break
			}
		}

		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		if bucketDowns.len <= idx || bucketDowns.at(idx).level == 0 {

			//if len(b.downLevels) <= idx || b.downLevels[idx].level == 0 || b.downLevels[idx].reverse == 0 {
			nidx := idx
			if nidx > bucketDowns.len-1 {
				nidx = bucketDowns.len - 1
			}
			for i := nidx; i > -1; i-- {
				if bucketDowns.at(i).level == 0 {
					continue
				}
				// FIXME: should not lookup direct
				if ignoreNoPool && bucketDowns.at(i)._itemPool == nil {
					continue
				}
				if ignoreNoInitDummy && bucketDowns.at(i).state != bucketStateActive {
					continue
				}

				b = bucketDowns.at(i).largestDown(ignoreNoPool, ignoreNoInitDummy)
				return b

			}
			break
		}
		// FIXME: should not lookup direct
		if ignoreNoPool && bucketDowns.at(idx)._itemPool == nil {
			break
		}
		if ignoreNoInitDummy && bucketDowns.at(idx).state != bucketStateActive {
			break
		}
		if !h.isEmbededItemInBucket {
			results = append(results, bucketDowns.at(idx))
		}

		b = bucketDowns.at(idx)
	}
	if b.level == 0 {
		return nil
	}
	return
}

func (b *bucket) parent(m *Map) (p *bucket) {

	for level := int32(1); level < b.level; level++ {
		if level == 1 {
			idx := (b.reverse >> (4 * 15))
			p = &m.buckets[idx]
			continue
		}
		idx := int((b.reverse >> (4 * (16 - level))) & 0xf)
		p = &p.downLevels[idx]
	}
	return
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

const recoverBucketWithOutInit bool = false

func (h *Map) bucketFromPool(reverse uint64, opts ...cOptFn) (b *bucket, onOk func()) {
	// h.mu.Lock()
	// defer h.mu.Unlock()

	opt := &commonOpt{}
	prevs := opt.Option(opts...)
	defer opt.Option(prevs...)

	level := int32(0)
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	bucketNotInits := []*bucket{}

	for l := int32(1); l <= level; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
	RETRY_INITIALIZE:
		if atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, 0, 1) {

			downLevels := make([]bucket, 1, 16)

			downLevels[0].level = b.level + 1
			downLevels[0].reverse = b.reverse
			downLevels[0].Init()
			downLevels[0].LevelHead.Init()
			downLevels[0]._parent = b
			downLevels[0].setItemPoolFn = func(p *samepleItemPool) {
				b.setItemPool(p)
			}

			lCur := h.levelBucket(l)
			if lCur.LevelHead.Empty() {
				lCur = bucketFromLevelHead(lCur.LevelHead.DirectPrev().DirectNext())
			}
			for ; lCur != lCur.NextOnLevel(); lCur = lCur.NextOnLevel() {
				if lCur.LevelHead.Empty() {
					break
				}
				if lCur.reverse < b.reverse {
					break
				}
			}
			lCur.LevelHead.InsertBefore(&downLevels[0].LevelHead)
			downLevels[0].state = bucketStateInit
			// if idx != 0 && !h.isEmbededItemInBucket {
			// 	h.add2(b.head(), &b.downLevels[0].dummy)
			// }
			b.downLevels = downLevels
		} else if len(b.downLevels) == 0 {
			if !recoverBucketWithOutInit {
				goto RETRY_INITIALIZE
			}
			if b.cntOfActiveLevels == 1 && cap(b.downLevels) > 1 {
				b.downLevels = b.downLevels[:b.cntOfActiveLevels]
			}
			goto RETRY_INITIALIZE
		}
	RETRY_SETUP:
		if b.cntOfActiveLevels <= int32(idx) && atomic.CompareAndSwapInt32(&b.cntOfActiveLevels, int32(len(b.downLevels)), int32(idx)+1) {
			cidx := int(b.cntOfActiveLevels) - 1
			if cidx > 32 || cidx < -32 {
				Log(LogWarn, "invalid cidx ")
			}

			nDownLevel := &b.downLevels[cidx : cidx+1 : cidx+1][0]
			nDownLevel.reverse = b.reverse | (uint64(cidx) << (4 * (16 - l)))
			nDownLevel.level = b.level + 1
			nDownLevel.state = bucketStateInit

			oBucket := b
			oIdx := cidx

			if onOk != nil {
				Log(LogWarn, "found old fn ")
			}
			nDownLevel.onOkFn = func() {
				if len(oBucket.downLevels) <= oIdx {
					oBucket.downLevels = oBucket.downLevels[:oIdx+1]
				} else {
					Log(LogDebug, "backet already expand ")
				}

				if !atomic.CompareAndSwapUint32(&oBucket.downLevels[oIdx].state, bucketStateInit, bucketStateActive) {
					Log(LogDebug, "fail bucket state change to finish ")
				}
			}
			onOk = nDownLevel.onOkFn

			if !opt.onOk {
				onOk()
				onOk = nil
				break
			}

			b = nDownLevel
			break
		} else if len(b.downLevels) <= idx {
			// for debug
			cDownLevels := b.downLevels[0:b.cntOfActiveLevels]
			last := &cDownLevels[b.cntOfActiveLevels-1]
			if !recoverBucketWithOutInit {
				goto RETRY_SETUP
			}

			if last.isRequireOnOk() {
				last.onOkFn()
			}

			goto RETRY_SETUP
		}

		if idx != 0 && atomic.CompareAndSwapUint32(&b.downLevels[idx].state, bucketStateNone, bucketStateInit) {
			if l != level {
				Log(LogWarn, "not collected already inited")
			}
			b.downLevels[idx].level = b.level + 1
			b.downLevels[idx].reverse = b.reverse | (uint64(idx) << (4 * (16 - l)))
			if onOk != nil {
				Log(LogWarn, "found old fn ")
			}

			oBucket := b
			oIdx := idx

			onOk = func() {
				if !atomic.CompareAndSwapUint32(&oBucket.downLevels[oIdx].state, bucketStateInit, bucketStateActive) {
					Log(LogWarn, "fail bucket state change to finish ")
				}
			}
			b.downLevels[idx].onOkFn = onOk

			b = &b.downLevels[idx]
			if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
				Log(LogWarn, "already inited")
			}
			break
		} else if idx != 0 && b.downLevels[idx].state != bucketStateActive {
			Log(LogWarn, "initializetion is not finished")
		}

		if !h.isEmbededItemInBucket && l < level && idx != 0 && b.downLevels[idx].state != bucketStateActive {
			bucketNotInits = append(bucketNotInits, &b.downLevels[idx])
		}
		if onOk != nil {
			Log(LogWarn, " skip okfn?")
		}
		b = &b.downLevels[idx]
	}
	if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
		h.DumpBucket(logio)
		Log(LogWarn, "already inited")
	}
	// obucketNotInits := bucketNotInits
	// _ = obucketNotInits
	// loncha.Delete(&bucketNotInits, func(i int) bool {
	// 	return b == bucketNotInits[i]
	// })
	// if len(bucketNotInits) > 0 {
	// 	h.setupBcukets(bucketNotInits)
	// }
	if onOk == nil {
		if b.onOkFn != nil {
			onOk = b.onOkFn
		} else {
			Log(LogWarn, "not set reverse?")
		}
	}
	return
}

// init bucket
//  bucketFromPool()            set bucket.level/ reverser
//  makeBucket()/makeBucket2()  init  as bucket elemet  .Init()
//                              init  as level element  .LevelHead.Init()
//    ->addBucket()             find previous bucket
//      -> _InsertBefore()      set dummy.fields
//                              connect dummy to item List
//                              connect bucket to bucket list
//  makeBucket()/makeBucket2()  connect bucket to level list

func (h *Map) setupBcukets(buckets []*bucket) {

	emptyListHead := list_head.ListHead{}

	for _, b := range buckets {
		if b.ListHead == emptyListHead {
			b.Init()
		}
		if b.LevelHead == emptyListHead {
			b.LevelHead.Init()
		}
		err := h.addBucket(b)
		if err != nil {
			Log(LogWarn, "fail addBucket() e=%v\n", err)
		}

		if !b.LevelHead.IsSingle() {
			nextLevel := h.findNextLevelBucket(b.reverse, b.level)

			if b.LevelHead.DirectNext() == &b.LevelHead {
				Log(LogWarn, "bucket.LevelHead is pointed to self")
			}
			if nextLevel != nil {
				nextLevelBucket := bucketFromLevelHead(nextLevel)
				if nextLevelBucket.reverse < b.reverse {
					nextLevel.InsertBefore(&b.LevelHead)
				} else if nextLevelBucket.reverse != b.reverse {
					nextLevel.DirectNext().InsertBefore(&b.LevelHead)
				}
			}
		}
	}

}

func (b *bucket) largestDown(ignoreNoPool, ignoreNoInitDummy bool) *bucket {

	if len(b.downLevels) == 0 {
		return b
	}

	for i := len(b.downLevels) - 1; i > -1; i-- {

		if b.downLevels[i].level == 0 || b.downLevels[i].reverse == 0 {
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
	// if atomic_util.LoadInt(&list.len) <= i {
	// 	return nil
	// }

	// data := atomic.LoadPointer(&list.data)
	// return (*bucket)(unsafe.Add(data, i*int(bucketSize)))
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
