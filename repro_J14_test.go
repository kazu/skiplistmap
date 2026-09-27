//go:build stephook

package skiplistmap_test

import (
	"sort"
	"testing"
	"time"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// j14Map is a map with the embedded pool whose bucket T of the keys has split
// once, so that downLevels[0] of T (firstDown) shares the item pool of T.
type j14Map struct {
	m      *WrapHMap
	stored []string // the keys set, in the order of their reversed hashes
	a      string   // the first key of the pool of T; its lookup finds firstDown
	b      string   // a key not stored, between a and the key after it in the pool of T
}

// newJ14Map sets 40 keys whose reversed hashes share the top 4 bits into a
// map with at most 16 items per bucket, and picks a and b.
func newJ14Map(t *testing.T) *j14Map {
	t.Helper()
	elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(list_head.Direct()) })

	m := newWrapHMap(skiplistmap.NewHMap(skiplistmap.UseEmbeddedPool(true), skiplistmap.MaxPefBucket(16)))
	var keys, rest []string
	for i := 0; len(keys)+len(rest) < 1000; i++ {
		k := crashKey(i)
		if reverseOf(k)>>60 != 3 {
			continue
		}
		if len(keys) < 40 {
			keys = append(keys, k)
		} else {
			rest = append(rest, k)
		}
	}
	for _, k := range keys {
		if !m.Set(k, &list_head.ListHead{}) {
			t.Fatalf("Set(%q) failed", k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return reverseOf(keys[i]) < reverseOf(keys[j]) })

	// The pool of a bucket holds its keys in the order of their reversed
	// hashes, so the first key of each base in that order is the first item
	// of its pool.
	seen := map[interface{}]bool{}
	for i, a := range keys {
		found, base := skiplistmap.StepLockBuckets(m.base, reverseOf(a))
		if seen[base] {
			continue
		}
		seen[base] = true
		if found == base || i+1 == len(keys) {
			continue
		}
		next := keys[i+1]
		if _, nbase := skiplistmap.StepLockBuckets(m.base, reverseOf(next)); nbase != base {
			continue
		}
		for _, b := range rest {
			if reverseOf(a) < reverseOf(b) && reverseOf(b) < reverseOf(next) {
				if _, bbase := skiplistmap.StepLockBuckets(m.base, reverseOf(b)); bbase == base {
					return &j14Map{m: m, stored: keys, a: a, b: b}
				}
			}
		}
	}
	t.Fatalf("no pool whose first key is found through firstDown with a free key after it")
	return nil
}

// rangeKeys returns the keys that RangeItem passes, or ok false if RangeItem
// does not return in time.
func (p *j14Map) rangeKeys() (keys []string, ok bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.m.base.RangeItem(func(item skiplistmap.MapItem) bool {
			keys = append(keys, item.Key().(string))
			return true
		})
	}()
	return keys, waitAtMost(done, 5*time.Second)
}

// J14 (3.2.4): ReplaceNext, which insertToPool uses to link the new array of
// a pool of skiplistmap5, puts back the links it read when its first CAS
// fails, although it changed nothing. It needs a delete in the pool that the
// insertion replaces, which the two locks of one pool (3.2.1) allow: Purge of
// a key found through firstDown locks firstDown.muPool, and Set of a new key
// locks the muPool of T, which owns the pool.
//
// Keys: a is the first item of the pool of T, b a new key after it in the
// same pool, n the node before a in the list of entries.
//
//  1. G1: Set(b) locks T.muPool, copies the pool of T into a new array with
//     b, and calls n.ReplaceNext(first, last, the node after the pool). It
//     reads n.next (a) and stops at "elist.replace.read".
//  2. G2: Purge(a) locks firstDown.muPool, which G1 does not hold, marks a,
//     unlinks it (n.next is now the item after a) and runs Init on a. The
//     list still holds every key but a.
//  3. G1 resumes: its first CAS, n.next from a to first, fails, and the
//     rollback stores the a it read into n.next. G1 stops at
//     "elist.replace.rollback".
//
// Now n.next is a, which Init linked to itself: RangeItem stops at a and
// misses every key after it, although only a was purged and the insertion
// had not changed any link.
func Test_Repro_J14_ReplaceNextRollbackRelinksPurgedItem(t *testing.T) {
	p := newJ14Map(t)
	want := map[string]bool{}
	for _, k := range p.stored {
		if k != p.a {
			want[k] = true
		}
	}
	check := func(when string) {
		t.Helper()
		keys, ok := p.rangeKeys()
		if !ok {
			t.Errorf("%s: RangeItem did not return in 5s", when)
			return
		}
		got := map[string]bool{}
		for _, k := range keys {
			got[k] = true
		}
		missing := 0
		for k := range want {
			if !got[k] {
				missing++
			}
		}
		if missing > 0 || got[p.a] {
			t.Errorf("%s: RangeItem passed %d keys; %d of the %d stored keys but a are missing, a present %v", when, len(keys), missing, len(want), got[p.a])
		}
	}

	s := newStepper(t)
	read := s.stopAt("elist.replace.read", nil)
	done1 := goStep(t, func() { p.m.Set(p.b, &list_head.ListHead{}) })
	read.waitReached(t, done1)

	var purged bool
	done2 := goStep(t, func() { purged = p.m.base.Purge(p.a) })
	if !waitAtMost(done2, 5*time.Second) {
		read.Release()
		t.Fatalf("Purge(a) waited for Set(b): the two did not take different locks")
	}
	if !purged {
		t.Fatalf("Purge(a) = false")
	}
	check("after Purge(a), with Set(b) stopped before its CASes")

	rollback := s.stopAt("elist.replace.rollback", nil)
	read.Release()
	rollback.waitReached(t, done1)
	check("after the rollback of the failed first CAS of Set(b)")

	rollback.Release()
	waitDone(t, done1, "G1 (Set(b))")
}
