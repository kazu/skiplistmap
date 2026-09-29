//go:build stephook

package skiplistmap_test

import (
	"testing"
	"time"

	"github.com/kazu/elist_head"
	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// directLista sets the traversal mode of lista, which is one variable for the
// whole process, to Direct, as New does, and puts it back to Direct when the
// test ends so that a mode left behind does not reach other tests.
func directLista(t *testing.T) {
	t.Helper()
	list_head.DefaultModeTraverse.SetType(list_head.TravDirect)
	t.Cleanup(func() { list_head.DefaultModeTraverse.SetType(list_head.TravDirect) })
}

// elistMode returns the traversal mode that elist shares across the process.
// SharedTrav returns the options that put the mode back; the first one is
// applied to a fresh ModeTraverse to read the old mode, and then all of them
// to elist to leave its mode as it was.
func elistMode() list_head.TraverseType {
	prevs := elist_head.SharedTrav(list_head.Direct())
	m := list_head.NewTraverse()
	prevs[0](m)
	elist_head.SharedTrav(prevs...)
	return m.Type()
}

// blockAtFirst returns a function for RangeItem that, on its first call,
// records the traversal mode of lista in before, closes reached, waits for
// release, records the mode again in after and stops the range.
func blockAtFirst(reached, release chan struct{}, before, after *list_head.TraverseType) func(skiplistmap.MapItem) bool {
	return func(skiplistmap.MapItem) bool {
		*before = list_head.DefaultModeTraverse.Type()
		close(reached)
		<-release
		*after = list_head.DefaultModeTraverse.Type()
		return false
	}
}

// The mode of lista starts at Direct. G1 calls RangeItem, which saves Direct,
// sets WaitNoMark and stops in f. G2 calls RangeItem, which saves WaitNoMark,
// sets WaitNoMark and stops in f. G1 returns and puts back Direct while G2 is
// still inside RangeItem, so G2 runs with Direct although it asked for
// WaitNoMark. G2 returns and puts back WaitNoMark. Both calls have returned
// and each put back what it saved, yet the mode of the whole process stays at
// WaitNoMark instead of Direct.
//
// Called one after the other, the two RangeItem calls leave Direct.
func Test_Repro_3_3_1_OverlappingRangeItemLeavesWaitNoMark(t *testing.T) {
	m := newStepMap()
	directLista(t)
	setKeys(t, m, adjacentKeys(1))

	var before1, after1, before2, after2 list_head.TraverseType
	reached1, release1 := make(chan struct{}), make(chan struct{})
	reached2, release2 := make(chan struct{}), make(chan struct{})
	done1 := goStep(t, func() { m.base.RangeItem(blockAtFirst(reached1, release1, &before1, &after1)) })
	waitClosed(t, reached1, done1, "RangeItem of G1")
	done2 := goStep(t, func() { m.base.RangeItem(blockAtFirst(reached2, release2, &before2, &after2)) })
	waitClosed(t, reached2, done2, "RangeItem of G2")

	close(release1)
	waitDone(t, done1, "RangeItem of G1")
	close(release2)
	waitDone(t, done2, "RangeItem of G2")

	if before2 != after2 {
		t.Errorf("the mode of lista inside RangeItem of G2 changed from %d to %d when G1 returned", before2, after2)
	}
	if got := list_head.DefaultModeTraverse.Type(); got != list_head.TravDirect {
		t.Errorf("after both RangeItem calls returned, the mode of lista is %d, want Direct (%d)", got, list_head.TravDirect)
	}
}

// The same two RangeItem calls one after the other put back Direct.
func Test_Repro_3_3_1_SequentialRangeItemKeepsDirect(t *testing.T) {
	m := newStepMap()
	directLista(t)
	setKeys(t, m, adjacentKeys(1))

	for i := 0; i < 2; i++ {
		m.base.RangeItem(func(skiplistmap.MapItem) bool { return false })
	}
	if got := list_head.DefaultModeTraverse.Type(); got != list_head.TravDirect {
		t.Errorf("after two RangeItem calls, the mode of lista is %d, want Direct (%d)", got, list_head.TravDirect)
	}
}

// One goroutine calls RangeItem once. RangeItem walks the list of entries,
// an elist list, with Prev and Next given WaitNoMark. elist sets its mode,
// which is one variable for the whole process, to WaitNoMark on each call and
// never puts it back, so after RangeItem returns the mode of elist stays at
// WaitNoMark.
func Test_Repro_3_3_1_RangeItemLeavesElistWaitNoMark(t *testing.T) {
	m := newStepMap()
	setKeys(t, m, adjacentKeys(1))
	prevs := elist_head.SharedTrav(list_head.Direct())
	t.Cleanup(func() { elist_head.SharedTrav(prevs...) })

	if got := elistMode(); got != list_head.TravDirect {
		t.Fatalf("before RangeItem, the mode of elist is %d, want Direct (%d)", got, list_head.TravDirect)
	}
	m.base.RangeItem(func(skiplistmap.MapItem) bool { return true })
	if got := elistMode(); got != list_head.TravDirect {
		t.Errorf("after RangeItem returned, the mode of elist is %d, want Direct (%d)", got, list_head.TravDirect)
	}
}

// waitClosed fails the test unless ch is closed before done.
func waitClosed(t *testing.T, ch, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-done:
		t.Fatalf("%s returned before it stopped", what)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not stop", what)
	}
}
