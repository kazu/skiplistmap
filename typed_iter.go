package skiplistmap

import "iter"

// All yields key/value pairs in Range order and stops when yield returns false.
func (h *Map[K, V]) All() iter.Seq2[K, V] { return h.Range }

// Keys yields keys in Range order without copying V.
func (h *Map[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		h.rangeItem(func(e *Entry[K, V]) bool { return yield(e.Key()) })
	}
}

// Values yields values in Range order.
func (h *Map[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		h.Range(func(_ K, value V) bool { return yield(value) })
	}
}
