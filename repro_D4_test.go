//go:build stephook

package skiplistmap

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	list_head "github.com/kazu/loncha/lista_encabezado"
)

// A map with embedded item pools (skiplistmap5) links its buckets into two
// kinds of lists: the list of buckets, and one level list per level. 3.3.2
// shows that the level lists can end up out of order. These tests show that
// the lookups of skiplistmap5 do not depend on the level lists: Get, LoadItem,
// LoadItemByHash, the existence check of Set, Purge, SearchKey and RangeItem
// find a bucket through the downLevels arrays (_findBucket) and the list of
// buckets (bsearchBybucket), and walk the list of entries (RangeItem). Only
// the insertion of a bucket into a level list (bucketFromPoolEmbedded and
// makeBucket2 through insertOnLevel) reads a level list.

// d4Map returns a map with embedded item pools that splits a bucket over 4
// items, holding n keys, and the keys with their values.
func d4Map(t *testing.T, n int) (*Map[StringKey, any],

	map[string]string) {
	t.Helper()
	h := New[StringKey, any](UseEmbeddedPool[StringKey, any](true), MaxPefBucket[StringKey, any](4))
	want := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("d4-%d", i)
		if !h.Set(StringKey(k), "v-"+k) {
			t.Fatalf("Set(%q) failed", k)
		}
		want[k] = "v-" + k
	}
	return h, want
}

// d4Levels is the number of level lists of a map.
const d4Levels = int32(len(Map[StringKey, any]{}.levels))

// d4LevelBuckets returns the buckets in the level list of level, walked
// forward from its head.
func d4LevelBuckets(t *testing.T, h *Map[StringKey, any],

	level int32) []*bucket[StringKey, any] {
	t.Helper()
	if h.isEmptyBylevel(level) {
		return nil
	}
	var got []*bucket[StringKey, any]

	head := h.levelBucket(level)
	for cur := head.LevelHead.DirectPrev().DirectNext(); !cur.Empty(); {
		b := bucketFromLevelHead[StringKey, any](cur)
		got = append(got, b)
		cur = b.LevelHead.DirectNext()
		if len(got) > 1<<20 {
			t.Fatalf("level %d list does not end", level)
		}
	}
	return got
}

// d4LevelLists returns the buckets of every level list; index level-1 holds
// the list of level.
func d4LevelLists(t *testing.T, h *Map[StringKey, any]) [][]*bucket[StringKey, any] {
	t.Helper()
	lists := make([][]*bucket[StringKey, any], d4Levels)
	for l := int32(1); l <= d4Levels; l++ {
		lists[l-1] = d4LevelBuckets(t, h, l)
	}
	return lists
}

// d4Relink rebuilds the level list of level so that it holds order, front
// to back, the way initBeforeSet links a bucket: before the first node.
// Buckets of the list left out of order are unlinked.
func d4Relink(t *testing.T, h *Map[StringKey, any],

	level int32, order []*bucket[StringKey, any]) {
	t.Helper()
	for _, b := range d4LevelBuckets(t, h, level) {
		b.LevelHead.Init()
	}
	head := h.levelBucket(level)
	head.LevelHead.InitAsEmpty()
	for i := len(order) - 1; i >= 0; i-- {
		b := order[i]
		b.LevelHead.Init()
		if _, err := head.LevelHead.DirectPrev().DirectNext().InsertBefore(&b.LevelHead); err != nil {
			t.Fatalf("relink level %d: %v", level, err)
		}
	}
	got := d4LevelBuckets(t, h, level)
	if len(got) != len(order) {
		t.Fatalf("relink level %d: %d buckets, want %d", level, len(got), len(order))
	}
	for i := range got {
		if got[i] != order[i] {
			t.Fatalf("relink level %d: bucket %d is %#x, want %#x", level, i, got[i].reverse, order[i].reverse)
		}
	}
}

// d4RelinkAll rebuilds every level list from lists.
func d4RelinkAll(t *testing.T, h *Map[StringKey, any],

	lists [][]*bucket[StringKey, any]) {
	t.Helper()
	for l := int32(1); l <= d4Levels; l++ {
		d4Relink(t, h, l, lists[l-1])
	}
}

// d4TableBuckets returns every bucket of the table: h.buckets and the
// claimed elements of their downLevels, recursively.
func d4TableBuckets(h *Map[StringKey, any]) []*bucket[StringKey, any] {
	var all []*bucket[StringKey, any]

	var walk func(b *bucket[StringKey, any])
	walk = func(b *bucket[StringKey, any]) {
		all = append(all, b)
		downs := b.ptrDownLevels()
		if downs == nil || downs.Cap() == 0 {
			return
		}
		for i := 0; i < downs.Len(); i++ {
			if d := downs.at(i); d != nil && d.level() != 0 {
				walk(d)
			}
		}
	}
	for i := range h.buckets {
		walk(&h.buckets[i])
	}
	return all
}

// d4Poison clears the LevelHead of every bucket of the table and of the head
// of every level list, so that following any level list dereferences nil.
// The returned function puts the LevelHeads back.
func d4Poison(h *Map[StringKey, any]) (restore func()) {
	saved := map[*bucket[StringKey, any]]list_head.ListHead{}
	for l := int32(1); l <= d4Levels; l++ {
		b := h.levelBucket(l)
		saved[b] = b.LevelHead
	}
	for _, b := range d4TableBuckets(h) {
		saved[b] = b.LevelHead
	}
	for b := range saved {
		b.LevelHead = list_head.ListHead{}
	}
	return func() {
		for b, lh := range saved {
			b.LevelHead = lh
		}
	}
}

// d4Panics reports whether fn panics.
func d4Panics(fn func()) (panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	fn()
	return false
}

// d4Check looks every key of want up with each lookup of skiplistmap5 and
// checks the result against want; the keys of gone must not be found.
// RangeItem must visit exactly the keys of want in ascending order of their
// reversed hashes, the order of the list of entries.
func d4Check(h *Map[StringKey, any],

	want map[string]string, gone []string) error {
	if h.Len() != len(want) {
		return fmt.Errorf("Len() = %d, want %d", h.Len(), len(want))
	}
	for k, v := range want {
		if got, ok := h.Get(StringKey(k)); !ok || got != v {
			return fmt.Errorf("Get(%q) = %v, %v; want %q, true", k, got, ok, v)
		}
		item, ok := h.LoadItemForTest(StringKey(k))
		if !ok || item.Key() != StringKey(k) || item.Value() != v {
			return fmt.Errorf("LoadItem(%q) failed: %v", k, ok)
		}
		hash, conflict := KeyToHash(k)
		item, ok = h.LoadItemByHashForTest(hash, conflict)
		if !ok || item.Key() != StringKey(k) || item.Value() != v {
			return fmt.Errorf("LoadItemByHash(%q) failed: %v", k, ok)
		}
		for _, ignore := range []bool{true, false} {
			e := h.searchKey(hash, ignore)
			if e == nil || e.PtrMapHead().reverse != bits.Reverse64(hash) {
				return fmt.Errorf("searchKey(%q, %v) did not find the key", k, ignore)
			}
		}
	}
	for _, k := range gone {
		if _, ok := h.Get(StringKey(k)); ok {
			return fmt.Errorf("Get(%q) found a purged key", k)
		}
		if _, ok := h.LoadItemForTest(StringKey(k)); ok {
			return fmt.Errorf("LoadItem(%q) found a purged key", k)
		}
		hash, _ := KeyToHash(k)
		if e := h.searchKey(hash, true); e != nil && e.PtrMapHead().reverse == bits.Reverse64(hash) {
			return fmt.Errorf("searchKey(%q, true) found a purged key", k)
		}
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	rev := func(k string) uint64 {
		hash, _ := KeyToHash(k)
		return bits.Reverse64(hash)
	}
	sort.Slice(keys, func(i, j int) bool { return rev(keys[i]) < rev(keys[j]) })
	var ranged []string
	h.RangeItemForTest(func(item *TestEntry[StringKey, any]) bool {
		ranged = append(ranged, string(item.Key()))
		return len(ranged) <= len(keys)
	})
	if fmt.Sprint(ranged) != fmt.Sprint(keys) {
		return fmt.Errorf("RangeItem visited %d keys, want %d in order of reverse", len(ranged), len(keys))
	}
	return nil
}

// d4SetMore sets n new keys from start and adds them to want.
func d4SetMore(t *testing.T, h *Map[StringKey, any],

	want map[string]string, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		k := fmt.Sprintf("d4-%d", i)
		if !h.Set(StringKey(k), "v-"+k) {
			t.Fatalf("Set(%q) failed", k)
		}
		want[k] = "v-" + k
	}
}

// The trap of the tests below. With the LevelHeads cleared by d4Poison, every
// reader of a level list panics: the walk from the head of a level list,
// isEmptyBylevel, NextOnLevel and
// PrevOnLevel (_set without a bucket, _searchBybucket, bucketFromPool,
// bucketFromPoolEmbedded), and so Set of new keys once it splits a bucket.
func Test_Repro_D4_PoisonedLevelListsTrapReaders(t *testing.T) {
	h, _ := d4Map(t, 2000)
	restore := d4Poison(h)
	b := h.findBucket(0x55 << 56)
	readers := []struct {
		name string
		fn   func()
	}{
		{"walk of the level 1 list", func() {
			head := h.levelBucket(1)
			_ = head.LevelHead.DirectPrev().DirectNext().DirectNext()
		}},
		{"isEmptyBylevel(2)", func() { h.isEmptyBylevel(2) }},
		{"NextOnLevel of the bucket of 0x55<<56", func() { b.NextOnLevel() }},
		{"PrevOnLevel of the bucket of 0x55<<56", func() { b.PrevOnLevel() }},
		{"NextOnLevel of h.buckets[5]", func() { h.buckets[5].NextOnLevel() }},
		// Last: a split links a new bucket into a level list, and the map is
		// not used after it panics.
		{"Set of new keys up to a split (makeBucket2)", func() {
			for i := 2000; i < 4000; i++ {
				h.Set(StringKey(fmt.Sprintf("d4-%d", i)), "v")
			}
		}},
	}
	for _, r := range readers {
		if !d4Panics(r.fn) {
			t.Errorf("%s did not panic on the cleared level lists", r.name)
		}
	}
	restore()
}

// Sequential. The level lists hold nothing a lookup can follow: d4Poison
// clears every LevelHead, so a read of any level list panics (see
// Test_Repro_D4_PoisonedLevelListsTrapReaders). Every lookup of skiplistmap5
// still returns the right result, Set of a present key stores the new value
// through its existence check, and Purge of half of the keys removes them.
// None of these reads a level list.
func Test_Repro_D4_LookupsDoNotReadLevelLists(t *testing.T) {
	h, want := d4Map(t, 2000)
	restore := d4Poison(h)
	defer restore()
	if err := d4Check(h, want, nil); err != nil {
		t.Fatalf("with the level lists cleared: %v", err)
	}
	for i := 0; i < 2000; i += 3 {
		k := fmt.Sprintf("d4-%d", i)
		if !h.Set(StringKey(k), "w-"+k) {
			t.Fatalf("Set(%q) of a present key failed", k)
		}
		want[k] = "w-" + k
	}
	if err := d4Check(h, want, nil); err != nil {
		t.Fatalf("after Set of present keys with the level lists cleared: %v", err)
	}
	var gone []string
	for i := 0; i < 2000; i += 2 {
		k := fmt.Sprintf("d4-%d", i)
		if !h.Purge(StringKey(k)) {
			t.Fatalf("Purge(%q) failed", k)
		}
		delete(want, k)
		gone = append(gone, k)
	}
	if err := d4Check(h, want, gone); err != nil {
		t.Fatalf("after Purge with the level lists cleared: %v", err)
	}
	restore()
	if err := StepCheckLists[StringKey, any](h); err != nil {
		t.Fatal(err)
	}
}

// d4Perms returns every order of bs.
func d4Perms(bs []*bucket[StringKey, any]) [][]*bucket[StringKey, any] {
	if len(bs) <= 1 {
		return [][]*bucket[StringKey, any]{append([]*bucket[StringKey, any](nil), bs...)}
	}
	var all [][]*bucket[StringKey, any]

	for i := range bs {
		rest := append(append([]*bucket[StringKey, any](nil), bs[:i]...), bs[i+1:]...)
		for _, p := range d4Perms(rest) {
			all = append(all, append([]*bucket[StringKey, any]{bs[i]}, p...))
		}
	}
	return all
}

// Sequential. A level list that concurrent insertions put out of order still
// holds the buckets of its level, each once, or misses some of them when an
// insertion has not linked its bucket yet. So every such list is an order of
// a subset of the buckets. The test puts the level lists in such orders:
// reversed, emptied, every order of a list of up to 6 buckets, and 100
// random orders of all of them at once, and the lookups stay right in each.
// Then new keys are set with the level lists reversed and with them emptied,
// so that splits link new buckets into the broken lists; the lookups, the
// list of entries and the list of buckets stay right.
func Test_Repro_D4_LookupsIgnoreLevelListOrder(t *testing.T) {
	h, want := d4Map(t, 2000)
	orig := d4LevelLists(t, h)
	for i, bs := range orig {
		if len(bs) > 0 {
			t.Logf("level %d list: %d buckets", i+1, len(bs))
		}
	}
	check := func(what string) {
		t.Helper()
		if err := d4Check(h, want, nil); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for l := int32(1); l <= d4Levels; l++ {
		bs := orig[l-1]
		if len(bs) == 0 {
			continue
		}
		rev := make([]*bucket[StringKey, any], len(bs))
		for i, b := range bs {
			rev[len(bs)-1-i] = b
		}
		d4Relink(t, h, l, rev)
		check(fmt.Sprintf("level %d reversed", l))
		d4Relink(t, h, l, nil)
		check(fmt.Sprintf("level %d emptied", l))
		if len(bs) <= 6 {
			for i, p := range d4Perms(bs) {
				d4Relink(t, h, l, p)
				check(fmt.Sprintf("level %d order %d", l, i))
			}
		}
		d4Relink(t, h, l, bs)
	}
	rng := rand.New(rand.NewSource(4))
	for n := 0; n < 100; n++ {
		lists := make([][]*bucket[StringKey, any], len(orig))
		for i, bs := range orig {
			lists[i] = append([]*bucket[StringKey, any](nil), bs...)
			rng.Shuffle(len(lists[i]), func(a, b int) { lists[i][a], lists[i][b] = lists[i][b], lists[i][a] })
			lists[i] = lists[i][:rng.Intn(len(lists[i])+1)]
		}
		d4RelinkAll(t, h, lists)
		check(fmt.Sprintf("random order %d", n))
	}

	reversed := make([][]*bucket[StringKey, any], len(orig))
	for i, bs := range orig {
		for j := len(bs) - 1; j >= 0; j-- {
			reversed[i] = append(reversed[i], bs[j])
		}
	}
	d4RelinkAll(t, h, reversed)
	nb := len(d4TableBuckets(h))
	d4SetMore(t, h, want, 2000, 2000)
	t.Logf("2000 Sets with the level lists reversed made %d buckets", len(d4TableBuckets(h))-nb)
	check("after 2000 Sets with the level lists reversed")
	if err := StepCheckLists[StringKey, any](h); err != nil {
		t.Fatalf("after 2000 Sets with the level lists reversed: %v", err)
	}
	d4RelinkAll(t, h, make([][]*bucket[StringKey, any], len(orig)))
	nb = len(d4TableBuckets(h))
	d4SetMore(t, h, want, 4000, 2000)
	t.Logf("2000 Sets with the level lists emptied made %d buckets", len(d4TableBuckets(h))-nb)
	check("after 2000 Sets with the level lists emptied")
	if err := StepCheckLists[StringKey, any](h); err != nil {
		t.Fatalf("after 2000 Sets with the level lists emptied: %v", err)
	}
}

// d4NibbleKeys returns n keys whose reversed hashes have top as their top 4
// bits, alternating below and above the middle of that range, so that the
// first split of the bucket of top can move keys to a new bucket.
func d4NibbleKeys(top uint64, n int) []string {
	var lo, hi []string
	for i := 0; len(lo) < n || len(hi) < n; i++ {
		k := fmt.Sprintf("d4n-%d", i)
		hash, _ := KeyToHash(k)
		r := bits.Reverse64(hash)
		if r>>60 != top {
			continue
		}
		if r>>59&1 == 0 {
			lo = append(lo, k)
		} else {
			hi = append(hi, k)
		}
	}
	var keys []string
	for i := 0; i < n; i++ {
		keys = append(keys, lo[i], hi[i])
	}
	return keys
}
