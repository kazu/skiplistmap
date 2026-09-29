//go:build stephook

package skiplistmap

import (
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// The helpers below are written with what the trees before the fixes also
// have, so that the J64 test builds there too.

// StepM27LevelReverses returns the reverses of the buckets on the level list
// of level, from its front to its last bucket, with no limit on the number of
// buckets.
func StepM27LevelReverses(h *Map, level int32) []uint64 {
	head := h.levelBucket(level)
	prevs := list_head.DefaultModeTraverse.Option(list_head.WaitNoM())
	front := head.LevelHead.Front()
	list_head.DefaultModeTraverse.Option(prevs...)
	var rs []uint64
	for cur := bucketFromLevelHead(front.DirectPrev().DirectNext()); !cur.LevelHead.Empty(); {
		rs = append(rs, cur.reverse)
		next := cur.NextOnLevel()
		if next == cur || len(rs) > 1<<20 {
			break
		}
		cur = next
	}
	return rs
}

// StepM27BucketLevel returns the level of the bucket p.
func StepM27BucketLevel(p unsafe.Pointer) int32 {
	return (*bucket)(p).level()
}

// StepM27LevelOf returns the level of the bucket that p, a LevelHead as
// makeBucket.levelFound reports it, belongs to, and the reverse of that bucket.
func StepM27LevelOf(p unsafe.Pointer) (level int32, reverse uint64) {
	b := bucketFromLevelHead((*list_head.ListHead)(p))
	return b.level(), b.reverse
}
