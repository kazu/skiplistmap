package typedapi

import "iter"

// RangeItems probes adapters over the typed RangeItem callback. The producer
// determines order and deletion visibility and must stop when yield is false.
type RangeItems[K Key[K], V any] func(yield func(*Entry[K, V]) bool)

func (r RangeItems[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		r(func(e *Entry[K, V]) bool { return yield(e.key, e.value) })
	}
}

func (r RangeItems[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		r(func(e *Entry[K, V]) bool { return yield(e.key) })
	}
}

func (r RangeItems[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		r(func(e *Entry[K, V]) bool { return yield(e.value) })
	}
}
