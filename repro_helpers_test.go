//go:build stephook

package skiplistmap_test

import (
	"reflect"
	"testing"
	"time"
)

// waitAtMost waits for done up to d and reports whether done was closed. It
// returns quietly when d passes, so that a goroutine that waits for a lock
// held by a stopped one does not fail the test.
func waitAtMost(done <-chan struct{}, d time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// waitFirst returns the index of the first of chans to be closed. It fails the
// test if none is closed in 10 seconds.
func waitFirst(t *testing.T, what string, chans ...<-chan struct{}) int {
	t.Helper()
	cases := make([]reflect.SelectCase, 0, len(chans)+1)
	for _, c := range chans {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c)})
	}
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(10 * time.Second))})
	i, _, _ := reflect.Select(cases)
	if i == len(chans) {
		t.Fatalf("%s neither finished nor stopped", what)
	}
	return i
}
