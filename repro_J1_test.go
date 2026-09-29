//go:build stephook

package skiplistmap_test

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kazu/skiplistmap"
)

// stdoutCapture collects what the map and its lists print with fmt.Print*
// while it runs. It replaces os.Stdout by a pipe, and stop puts it back.
type stdoutCapture struct {
	old  *os.File
	w    *os.File
	done chan struct{}
	mu   sync.Mutex
	buf  bytes.Buffer
	once sync.Once
}

func (c *stdoutCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// String returns what was printed so far.
func (c *stdoutCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func startStdoutCapture(t *testing.T) *stdoutCapture {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	c := &stdoutCapture{old: os.Stdout, w: w, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		io.Copy(c, r)
		r.Close()
	}()
	os.Stdout = w
	t.Cleanup(func() { c.stop() })
	return c
}

// stop puts os.Stdout back and returns everything printed while c ran.
func (c *stdoutCapture) stop() string {
	c.once.Do(func() {
		os.Stdout = c.old
		c.w.Close()
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
		}
	})
	return c.String()
}

// The concurrent test of the report (Test_ConcurrentStoreItem) printed
// "invalid item order" in all 15 runs. _searchBybucket prints it when it
// walks the entry list forward from a bucket and meets a key whose reverse
// is smaller than that of the key before it, so the entry list really held
// keys out of order.
//
// Keys: a < b < c < d in the order of their reversed hashes, all in 0x31..,
// in a map that does not split buckets. A search for them starts at the
// nearer of the buckets 0x30.. and 0x40.., which is 0x30.., and walks forward.
//
// Interleaving (the one of Test_StepRetryAfterFailedCASKeepsOrder): the list
// holds a and d. G1 stores b: add2 finds d, the order check passes with a
// before d, and G1 stops at elist.add.cas1 before its first CAS. G2 stores c
// between a and d. G1 resumes: its CAS on a.next fails, InsertBefore reads c
// as the prev of d and links b between c and d without an order check, so
// the list is a, c, b, d. A Get of d then walks a, c, b forward from the
// dummy of the bucket 0x30.. and prints "invalid item order" at b.
func Test_ReproJ1SearchSeesKeysOutOfOrder(t *testing.T) {
	keys := regionKeys(0x3, 0x1, 4)
	items := newStepItems(keys)
	m := newStepMap()
	m.base.StoreItem(&items[0])
	m.base.StoreItem(&items[3])

	s := newStepper(t)
	stop := s.stopAt("elist.add.cas1", isNode(nodeOf(&items[1])))
	done := goStep(t, func() { m.base.StoreItem(&items[1]) })
	stop.waitReached(t, done)
	m.base.StoreItem(&items[2])
	stop.Release()
	waitDone(t, done, "StoreItem(b)")

	out := startStdoutCapture(t)
	for _, k := range keys {
		m.Get(k)
	}
	printed := out.stop()

	var got []string
	runWithDeadline(t, 10*time.Second, func() {
		m.base.RangeItem(func(item skiplistmap.MapItem) bool {
			got = append(got, item.Key().(string))
			return true
		})
	})
	name := map[string]string{keys[0]: "a", keys[1]: "b", keys[2]: "c", keys[3]: "d"}
	var order []string
	for _, k := range got {
		order = append(order, name[k])
	}
	if n := strings.Count(printed, "invalid item order"); n > 0 {
		t.Errorf("Get of the keys printed \"invalid item order\" %d times: the entry list holds %v", n, order)
	}
	runtime.KeepAlive(items)
}
