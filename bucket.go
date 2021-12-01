package skiplistmap

import (
	"errors"
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

	cntFn := func(l int) int {
		r := 4
		for i := 1; i < l+1; i++ {
			r *= r
		}
		return r
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

func (buf *bucketBuffer) allocLevel(level int) error {

	if len(buf.buf) == 0 {
		return errors.New("cannot allocate in empty buffer")
	}

	b := &buf.buf[0]
	if b.level != 1 {
		return errors.New("must allocate in level 1 buffer")
	}

	if level >= 16 {
		return errors.New("level < 16")
	}

	for cur := buf; cur.level() < level; cur = cur.Next() {
		if cur.level() == level-1 {
			if cur.Next() != cur {
				return errors.New("alread allocated")
			}
			nbuf := newBucketBuffer(level)
			cur.ListHead.Next().InsertBefore(&nbuf.ListHead)
			buf.max = level
			return nil
		}
	}
	return errors.New("not allocated before-level")
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

	downLevel *list_head.ListHead // to bucket.Levelhead

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

func (b *bucket) registerToUpLevel() {

	if b.level > 1 {
		var upBucket *bucket
		upBucket = nil
		_ = upBucket
		if nextBuckketOnLevel := b.NextOnLevel(); nextBuckketOnLevel != b {
			for cur := b; cur.reverse > nextBuckketOnLevel.reverse; cur = cur.nextAsB() {

				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
				if cur == cur.nextAsB() {
					break
				}
			}
		} else if nextBucket := b.nextAsB(); nextBucket != b {
			for cur := nextBucket; cur.level != b.level; cur = cur.nextAsB() {
				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
				if cur == cur.nextAsB() {
					break
				}
			}
		} else {

			for cur := b.prevAsB(); cur.level != b.level; cur = cur.prevAsB() {

				if cur.level == b.level-1 {
					upBucket = cur
					break
				}
				if cur == cur.prevAsB() {
					break
				}
			}
		}

		if upBucket != nil {
			upBucket.downLevel = &b.LevelHead
		} else {
			Log(LogDebug, "bucket=%p set downlevel to upper level bucket", b)
		}

	}

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
