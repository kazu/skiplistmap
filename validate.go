package skiplistmap

func (h *Map[K, V]) validateBucket(btable *bucket[K, V]) (succ bool) {

	succ = h.toFrontBucket(btable).DirectPrev() != h.headBucket ||
		h.toBackBucket(btable).DirectNext() != h.tailBucket
	if !succ {
		fb := h.toFrontBucket(btable)
		bb := h.toBackBucket(btable)
		_, _ = fb, bb
	}
	return
}
