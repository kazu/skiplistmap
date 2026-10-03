package typedapi

import (
	"maps"
	"slices"
	"testing"
)

func TestRangeOverFunc(t *testing.T) {
	entries := []*Entry[StringKey, int]{NewEntry(StringKey("b"), 2), NewEntry(StringKey("a"), 1)}
	visits := 0
	rangeItems := RangeItems[StringKey, int](func(yield func(*Entry[StringKey, int]) bool) {
		for _, e := range entries {
			visits++
			if !yield(e) {
				return
			}
		}
	})
	if got := maps.Collect(rangeItems.All()); !maps.Equal(got, map[StringKey]int{"a": 1, "b": 2}) {
		t.Fatalf("maps.Collect: %v", got)
	}
	if got := slices.Collect(rangeItems.Keys()); !slices.Equal(got, []StringKey{"b", "a"}) {
		t.Fatalf("iteration changed producer order: %v", got)
	}
	if got := slices.Sorted(rangeItems.Keys()); !slices.Equal(got, []StringKey{"a", "b"}) {
		t.Fatalf("slices.Sorted: %v", got)
	}
	if got := slices.Collect(rangeItems.Values()); !slices.Equal(got, []int{2, 1}) {
		t.Fatalf("values: %v", got)
	}
	visits = 0
	for range rangeItems.All() {
		break
	}
	if visits != 1 {
		t.Fatalf("All failed to stop producer: %d visits", visits)
	}
	visits = 0
	for range rangeItems.Keys() {
		break
	}
	if visits != 1 {
		t.Fatalf("Keys failed to stop producer: %d visits", visits)
	}
	visits = 0
	for range rangeItems.Values() {
		break
	}
	if visits != 1 {
		t.Fatalf("Values failed to stop producer: %d visits", visits)
	}
}
