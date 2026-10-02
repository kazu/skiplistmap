package skiplistmap

// These test-only methods let structural regressions inspect pool slots without
// exposing them through the public API of an embedded map.
type TestEntry[K Key[K], V any] = embeddedEntry[K, V]

func (h *Map[K, V]) LoadItemForTest(key K) (*TestEntry[K, V], bool) {
	hash, conflict := key.KeyHash()
	e, _, found := h.getItemWithBucket(hash, conflict, key, true)
	return e, found
}

func (h *Map[K, V]) LoadItemByHashForTest(hash, conflict uint64) (*TestEntry[K, V], bool) {
	return h._get(hash, conflict)
}

func (h *Map[K, V]) RangeItemForTest(f func(*TestEntry[K, V]) bool) {
	h.rangeItem(f)
}

func (h *Map[K, V]) StoreItemForTest(e *TestEntry[K, V]) bool {
	ok, _ := h.storeItem(e)
	return ok
}

func (h *Map[K, V]) StoreItemOrCopyForTest(e *TestEntry[K, V]) (*Entry[K, V], bool) {
	return h.storeItemOrCopy(e)
}

func (h *Map[K, V]) SetEntryForTest(k, conflict uint64, b *bucket[K, V], e *TestEntry[K, V]) bool {
	return h._set(k, conflict, b.toBase(), e)
}
