package skiplistmap

// These test-only methods let structural regressions inspect pool slots without
// exposing them through the public API of an embedded map.
func (h *Map[K, V]) LoadItemForTest(key K) (*Entry[K, V], bool) {
	hash, conflict := key.KeyHash()
	e, _, found := h.getItemWithBucket(hash, conflict, key, true)
	return e, found
}

func (h *Map[K, V]) LoadItemByHashForTest(hash, conflict uint64) (*Entry[K, V], bool) {
	return h._get(hash, conflict)
}

func (h *Map[K, V]) RangeItemForTest(f func(*Entry[K, V]) bool) {
	h.rangeItem(f)
}
