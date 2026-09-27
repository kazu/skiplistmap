//go:build stephook

package skiplistmap_test

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// The concurrent test of the report (Test_ConcurrentStoreItem) printed the
// lista warning "Warn: list_head insert element must be single node" in 12
// of 15 runs. lista's insertBefore prints it when the node it inserts is
// already linked, so a bucket was inserted into the list of buckets twice.
//
// Interleaving (the two splits of startTwoSplits): the winner G1 and the
// loser G2 of the state CAS stop at makeBucket.claimed with the new bucket b
// (reverse 0x34..). Before the fix G2 goes on: the dummy D of b is still
// empty, so G2 initializes b, finds the bucket t to insert b before,
// initializes D and stops at add2.found for D. G1 then runs to the end: it
// links D, and links b before t in the list of buckets. G2 resumes: it finds
// D linked, deletes and links it again, and inserts b before t once more.
// b is already linked, so lista prints the warning.
func Test_ReproJ2LoserInsertsLinkedBucketWarns(t *testing.T) {
	sp := startTwoSplits(t, nil)
	s, b := sp.s, sp.b
	if r := skiplistmap.StepBucketReverse(b) >> 56; r != 0x34 {
		t.Fatalf("the new bucket has reverse %02x.., want 34..", r)
	}
	dummy := skiplistmap.StepBucketDummy(b)

	out := startStdoutCapture(t)
	loserDone := false
	if sp.lose.a == nil {
		// The loser leaves the split to the winner.
		sp.lose.Release()
		waitDone(t, sp.loseDone, "loser")
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		loserDone = true
	} else {
		found := s.stopAt("map.add2.found", isNode(dummy))
		sp.lose.Release()
		found.waitReached(t, sp.loseDone)
		sp.win.Release()
		waitDone(t, sp.winDone, "winner")
		found.Release()
		select {
		case <-sp.loseDone:
			loserDone = true
		case <-time.After(10 * time.Second):
		}
	}
	printed := out.stop()

	if n := strings.Count(printed, "list_head insert element must be single node"); n > 0 {
		t.Errorf("the splits printed the lista warning \"insert element must be single node\" %d times: a linked bucket was inserted into the list of buckets again", n)
	}
	if !loserDone {
		t.Fatalf("the loser did not finish")
	}
	runtime.KeepAlive(sp.items)
}
