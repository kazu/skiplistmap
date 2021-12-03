package skiplistmap

func (h *Map) validateBucket(btable *bucket) (succ bool) {

	succ = h.toFrontBucket(btable).DirectPrev() != h.firstBucket ||
		h.toBackBucket(btable).DirectNext() != h.lastBucket
	if !succ {
		fb := h.toFrontBucket(btable)
		bb := h.toBackBucket(btable)
		_, _ = fb, bb
	}
	return
}
