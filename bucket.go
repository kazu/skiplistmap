package skiplistmap

import (
	"errors"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
)

type bucket struct {
	level   int32
	_len    int32
	reverse uint64
	dummy   entryHMap
	head    *elist_head.ListHead // to MapEntry

	downLevels []bucket

	_itemPool     *samepleItemPool
	_parent       *bucket
	itemPoolFn    func() *samepleItemPool
	setItemPoolFn func(*samepleItemPool)
	headPool      list_head.ListHead
	tailPool      list_head.ListHead

	muPool sync.Mutex

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
	// MENTION: should not run this
	return nil
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

	if b.head == nil {
		return nil
	}
	head := b.head
	if !head.DirectNext().Empty() {
		head = head.DirectNext()
	}

	if !head.Empty() {
		return entryHMapFromListHead(head)
	}

	return nil

}

func (b *bucket) PrevEntry() *entryHMap {

	if b.head == nil {
		return nil
	}
	head := b.head
	if !head.DirectPrev().Empty() {
		head = head.DirectPrev()
	}

	if !head.Empty() {
		return entryHMapFromListHead(head)
	}

	return nil

}

func (b *bucket) entry(h *Map) (e HMapEntry) {

	if b.head == nil {
		return nil
	}
	head := b.head
	if !head.Empty() {
		return h.ItemFn().HmapEntryFromListHead(head)
	}
	return b.NextEntry()

}

func (h *Map) FindBucket(reverse uint64) (b *bucket) {
	return h._findBucket(reverse, false)
}

func (h *Map) findBucket(reverse uint64) (b *bucket) {

	return h._findBucket(reverse, false)
}

func (h *Map) _findBucket(reverse uint64, ignoreNoPool bool) (b *bucket) {

	for l := 1; l < 16; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		if len(b.downLevels) <= idx || b.downLevels[idx].level == 0 {

			//if len(b.downLevels) <= idx || b.downLevels[idx].level == 0 || b.downLevels[idx].reverse == 0 {
			nidx := idx
			if nidx > len(b.downLevels)-1 {
				nidx = len(b.downLevels) - 1
			}
			for i := nidx; i > -1; i-- {
				if b.downLevels[i].level == 0 {
					continue
				}
				// FIXME: should not lookup direct
				if !ignoreNoPool || b.downLevels[i]._itemPool != nil {
					b = b.downLevels[i].largestDown(ignoreNoPool)
					return b
				}
			}
			break
		}
		// FIXME: should not lookup direct
		if ignoreNoPool && b.downLevels[idx]._itemPool == nil {
			break
		}

		b = &b.downLevels[idx]
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

func (b *bucket) toBase() *bucket {

	if b._parent == nil {
		return b
	}
	return b._parent.toBase()
}

func (h *Map) bucketFromPool(reverse uint64) (b *bucket) {

	level := int32(0)
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	for l := int32(1); l <= level; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		if cap(b.downLevels) == 0 {
			b.downLevels = make([]bucket, 1, 16)
			b.downLevels[0].level = b.level + 1
			b.downLevels[0].reverse = b.reverse
			b.downLevels[0].head = b.head
			b.downLevels[0].Init()
			b.downLevels[0].LevelHead.Init()
			b.downLevels[0]._parent = b
			// b.downLevels[0].itemPoolFn = func() *samepleItemPool {
			// 	return .itemPool()
			// }
			b.downLevels[0].setItemPoolFn = func(p *samepleItemPool) {
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
			lCur.LevelHead.InsertBefore(&b.downLevels[0].LevelHead)
		}
		if len(b.downLevels) <= idx {
			b.downLevels = b.downLevels[:idx+1]
		}

		if b.downLevels[idx].level == 0 {
			if l != level {
				Log(LogWarn, "not collected already inited")
			}
			b.downLevels[idx].level = b.level + 1
			b.downLevels[idx].reverse = b.reverse | (uint64(idx) << (4 * (16 - l)))
			b = &b.downLevels[idx]
			if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
				Log(LogWarn, "already inited")
			}
			break
		}
		b = &b.downLevels[idx]
	}
	if b.ListHead.DirectPrev() != nil || b.ListHead.DirectNext() != nil {
		h.DumpBucket(logio)
		Log(LogWarn, "already inited")
	}
	return
}

func (b *bucket) largestDown(ignoreNoPool bool) *bucket {

	if len(b.downLevels) == 0 {
		return b
	}

	for i := len(b.downLevels) - 1; i > -1; i-- {

		if b.downLevels[i].level == 0 || b.downLevels[i].reverse == 0 {
			continue
		}
		//FIXME: should not lookup direct
		if !ignoreNoPool || b.downLevels[i]._itemPool != nil {
			return b.downLevels[i].largestDown(ignoreNoPool)
		}
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
