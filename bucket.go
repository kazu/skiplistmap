package skiplistmap

import (
	"errors"
	"math/bits"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

var EmptyBucketBuffer bucketBuffer = bucketBuffer{}

type bucketBuffer struct {
	start *list_head.ListHead
	last  *list_head.ListHead

	buf []bucket
	max int

	list_head.ListHead
}

func (buf *bucketBuffer) Offset() uintptr {
	return unsafe.Offsetof(buf.ListHead)
}

func (buf *bucketBuffer) FromListHead(l *list_head.ListHead) list_head.List {
	return buf.fromListHead(l)
}

func (buf *bucketBuffer) fromListHead(l *list_head.ListHead) *bucketBuffer {
	return (*bucketBuffer)(list_head.ElementOf(&EmptyBucketBuffer, l))
}

func (buf *bucketBuffer) PtrListHead() *list_head.ListHead {
	return &buf.ListHead
}

func (buf *bucketBuffer) Next() *bucketBuffer {
	next := buf.ListHead.Next()
	if next == next.Next() {
		return buf
	}

	return buf.fromListHead(next)
}

func (buf *bucketBuffer) Prev() *bucketBuffer {
	prev := buf.ListHead.Prev()
	if prev == prev.Prev() {
		return buf
	}

	return buf.fromListHead(prev)
}

func newBucketBuffer(level int) (buf *bucketBuffer) {

	buf = &bucketBuffer{}
	buf.Init()

	cntFn := func(l int) (r int) {

		r = 0
		for i := 0; i < l; i++ {
			r = (r << 4) | 0xf
		}

		r++
		return
	}
	size := cntFn(level)
	buf.buf = make([]bucket, size)
	buf.buf[0].level = level

	head := &list_head.ListHead{}
	head.InitAsEmpty()
	buf.start = head.DirectPrev()
	buf.last = head.DirectNext()

	return buf
}

func (buf *bucketBuffer) level() int {

	if len(buf.buf) == 0 {
		return -1
	}
	return buf.buf[0].level
}

func (buf *bucketBuffer) fromLevel(level int) *bucketBuffer {

	for cur := buf; cur.level() <= level; cur = cur.Next() {
		if cur.level() == level {
			return cur
		}
		if cur == cur.Next() {
			break
		}
	}
	return nil
}

func (buf *bucketBuffer) fromReverse(reverse uint64, level int, check bool) *bucket {

	lbuf := buf.fromLevel(level)
	if lbuf == nil {
		return nil
	}
	r := &lbuf.buf[reverse2Index(level, reverse)]
	if !check {
		return r
	}
	if r.level == 0 {
		return nil
	}
	if r.level != 1 && r.reverse == 0 {
		return nil
	}
	return r
}
func (buf *bucketBuffer) nearReverse(reverse uint64, level int) (r *bucket) {

	lbuf := buf.fromLevel(level)
	if lbuf == nil {
		return nil
	}
	r = &lbuf.buf[reverse2Index(level, reverse)]

	if r == nil || r.level == 0 || r.reverse == 0 {
		for i := 1; i < 16; i++ {
			if len(lbuf.buf) <= reverse2Index(level, reverse)+i {
				break
			}
			c := &lbuf.buf[reverse2Index(level, reverse)+i]
			if c.level != 0 {
				r = c
				break
			}
		}
	}
	if r == nil {
		return nil
	}

	if r.level == 0 {
		return nil
	}
	if r.level != 1 && r.reverse == 0 {
		return nil
	}
	return r

}

func (buf *bucketBuffer) maxLevel() (max int) {

	if buf.max > 0 {
		return buf.max
	}

	for cur := buf; cur.level() < 16; cur = cur.Next() {
		max = cur.level()
		if cur == cur.Next() {
			break
		}
	}
	return max

}

type bucket struct {
	level   int
	reverse uint64
	len     int64
	start   *list_head.ListHead // to MapEntry

	downLevels []bucket

	LevelHead list_head.ListHead // to same level bucket
	list_head.ListHead
}

func (e *bucket) Offset() uintptr {
	return unsafe.Offsetof(e.ListHead)
}

func (e *bucket) OffsetLevel() uintptr {
	return unsafe.Offsetof(e.LevelHead)
}

func (e *bucket) PtrListHead() *list_head.ListHead {
	return &e.ListHead
}

func (e *bucket) PtrLevelHead() *list_head.ListHead {
	return &e.LevelHead
}

func (e *bucket) FromListHead(head *list_head.ListHead) list_head.List {
	return entryHMapFromListHead(head)
}

func bucketFromListHead(head *list_head.ListHead) *bucket {
	return (*bucket)(list_head.ElementOf(emptyBucket, head))
}

func bucketFromLevelHead(head *list_head.ListHead) *bucket {
	if head == nil {
		return nil
	}
	return (*bucket)(unsafe.Pointer(uintptr(unsafe.Pointer(head)) - emptyBucket.OffsetLevel()))
}

func (b *bucket) checklevel() error {

	level := -1
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

	//if b.ListHead.DirectPrev().Empty() {
	prev := b.ListHead.Prev()
	if prev == prev.DirectPrev() {
		//if prev.Empty() {
		return b
	}

	//return bucketFromListHead(b.ListHead.DirectPrev())
	return bucketFromListHead(prev)

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

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.DirectNext().Empty() {
		start = start.DirectNext()
	}

	if !start.Empty() {
		return entryHMapFromListHead(start)
	}

	return nil

}

func (b *bucket) PrevEntry() *entryHMap {

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.DirectPrev().Empty() {
		start = start.DirectPrev()
	}

	if !start.Empty() {
		return entryHMapFromListHead(start)
	}

	return nil

}

func (b *bucket) entry(h *Map) (e HMapEntry) {

	if b.start == nil {
		return nil
	}
	start := b.start
	if !start.Empty() {
		return h.ItemFn().HmapEntryFromListHead(start)
	}
	return b.NextEntry()

}

func (h *Map) findBucket(reverse uint64) (b *bucket) {

	for l := 1; l < 16; l++ {
		if l == 1 {
			idx := (reverse >> (4 * 15))
			b = &h.buckets[idx]
			continue
		}
		idx := int((reverse >> (4 * (16 - l))) & 0xf)
		if len(b.downLevels) <= idx {
			break
		}
		if b.downLevels[idx].level == 0 || b.downLevels[idx].reverse == 0 {
			break
		}
		b = &b.downLevels[idx]
	}
	if b.level == 0 {
		return nil
	}
	return
}

func (h *Map) bucketFromPool(reverse uint64) (b *bucket) {

	level := 0
	for cur := bits.Reverse64(reverse); cur != 0; cur >>= 4 {
		level++
	}

	for l := 1; l <= level; l++ {
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
			b.downLevels[0].start = b.start
			b.downLevels[0].Init()
			b.downLevels[0].LevelHead.Init()
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
