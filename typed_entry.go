package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// Entry holds a key and value directly. Use a pointer as V to refer to an
// existing value instead. Keep caller-owned entries alive while linked.
type Entry[K Key[K], V any] struct {
	key   K
	value V
	MapHead
	root     *Entry[K, V]
	retained atomic.Pointer[Entry[K, V]]
	previous *Entry[K, V]
	reusable bool
	readers  atomic.Int64
	ready    atomic.Uint32
}

// NewEntry creates a caller-owned root. Updates retain copies from this root;
// an earlier entry keeps its earlier value after replacement.
func NewEntry[K Key[K], V any](key K, value V) *Entry[K, V] {
	e := &Entry[K, V]{}
	e.InitEntry(key, value)
	return e
}

// Copy returns a new caller-owned entry with a shallow copy of e's key and
// value. It shares any referenced data, but not links, deletion history or
// retained replacements. An enclosing struct is not copied.
func (e *Entry[K, V]) Copy() *Entry[K, V] {
	key, value, _ := e.loadTypedKeyValue()
	return NewEntry(key, value)
}

// InitEntry initializes a zero Entry in place, including an Entry embedded in
// a caller-owned struct or array. It returns false if already initialized.
// Do not copy an Entry after initialization.
func (e *Entry[K, V]) InitEntry(key K, value V) bool {
	if !e.ready.CompareAndSwap(0, 1) {
		return false
	}
	e.key, e.value = key, value
	if !e.reusable {
		e.root = e
	}
	e.ready.Store(2)
	return true
}

func (e *Entry[K, V]) Key() K {
	if !e.waitPayload() {
		var zero K
		return zero
	}
	e.acquireRead()
	key := e.key
	e.releaseRead()
	return key
}
func (e *Entry[K, V]) Value() V {
	if !e.waitPayload() {
		var zero V
		return zero
	}
	e.acquireRead()
	value := e.value
	e.releaseRead()
	return value
}
func (e *Entry[K, V]) KeyHash() (uint64, uint64) { return e.Key().KeyHash() }
func (e *Entry[K, V]) PtrMapHead() *MapHead      { return &e.MapHead }

func (e *Entry[K, V]) Offset() uintptr {
	return unsafe.Offsetof(e.MapHead) + unsafe.Offsetof(e.MapHead.ListHead)
}

func (e *Entry[K, V]) storeTypedKeyValue(key K, value V) {
	if e.ready.Load() == 0 {
		e.initializePayload(key, value, nil, e.reusable)
		return
	}
	e.waitPayload()
	e.acquireWrite()
	atomic.AddUint64((*uint64)(&e.state), uint64(mapKeyWriting))
	e.key = key
	e.value = value
	atomic.AddUint64((*uint64)(&e.state), uint64(mapKeyWriting))
	e.releaseWrite()
}

func (e *Entry[K, V]) loadTypedKeyValue() (K, V, mapState) {
	return e.loadKeyValue(true)
}

func (e *Entry[K, V]) loadKeyValue(withValue bool) (K, V, mapState) {
	if !e.waitPayload() {
		var key K
		var value V
		return key, value, mapIsDummy
	}
	e.acquireRead()
	key, state := e.key, mapState(atomic.LoadUint64((*uint64)(&e.state)))
	var value V
	if withValue {
		value = e.value
	}
	e.releaseRead()
	if stepEnabled {
		stepAt("key.dataRead", unsafe.Pointer(e.PtrListHead()), nil)
	}
	return key, value, state
}

func (e *Entry[K, V]) waitPayload() bool {
	for {
		state := e.ready.Load()
		if state != 1 {
			return state == 2
		}
		runtime.Gosched()
	}
}

func (e *Entry[K, V]) initializePayload(key K, value V, root *Entry[K, V], reusable bool) {
	if !e.ready.CompareAndSwap(0, 1) {
		e.waitPayload()
		return
	}
	e.key, e.value = key, value
	e.reusable = reusable
	if !reusable {
		e.root = root
		if e.root == nil {
			e.root = e
		}
	}
	e.ready.Store(2)
}

func (e *Entry[K, V]) acquireRead() {
	if !e.reusable {
		return
	}
	for {
		n := e.readers.Load()
		if n >= 0 && e.readers.CompareAndSwap(n, n+1) {
			return
		}
		runtime.Gosched()
	}
}

func (e *Entry[K, V]) releaseRead() {
	if e.reusable {
		e.readers.Add(-1)
	}
}

func (e *Entry[K, V]) acquireWrite() {
	if e.reusable {
		for !e.readers.CompareAndSwap(0, -1) {
			runtime.Gosched()
		}
	}
}

func (e *Entry[K, V]) releaseWrite() {
	if e.reusable {
		e.readers.Store(0)
	}
}

func (e *Entry[K, V]) clearRetiredValue() {
	e.acquireWrite()
	var zero V
	e.value = zero
	e.releaseWrite()
}

func newEntryList[K Key[K], V any]() elist_head.List[Entry[K, V]] {
	var e Entry[K, V]
	return elist_head.NewList[Entry[K, V]](e.Offset())
}
