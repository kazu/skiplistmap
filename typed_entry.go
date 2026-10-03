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
	embeddedEntry[K, V]
	retainedEntry *Entry[K, V]
}

type embeddedEntry[K Key[K], V any] struct {
	key   K
	value V
	MapHead
}

// NewEntry creates a caller-owned entry. An earlier entry keeps its earlier
// value after replacement.
func NewEntry[K Key[K], V any](key K, value V) *Entry[K, V] {
	e := &Entry[K, V]{}
	e.InitEntry(key, value)
	return e
}

// Copy returns a new caller-owned entry with a shallow copy of e's key and
// value. It shares any referenced data, but not links, deletion history or
// retained replacements. An enclosing struct is not copied.
func (e *embeddedEntry[K, V]) Copy() *Entry[K, V] {
	key, value, _ := e.loadTypedKeyValue()
	return NewEntry(key, value)
}

// InitEntry initializes a zero Entry in place, including an Entry embedded in
// a caller-owned struct or array. It returns false if already initialized.
// Do not copy an Entry after initialization.
func (e *embeddedEntry[K, V]) InitEntry(key K, value V) bool {
	if !e.claimPayload() {
		return false
	}
	e.key, e.value = key, value
	atomic.AddUint64((*uint64)(&e.state), uint64(mapPayloadInitializing))
	return true
}

func (e *embeddedEntry[K, V]) Key() K {
	if !e.waitPayload() {
		var zero K
		return zero
	}
	e.acquireRead()
	key := e.key
	e.releaseRead()
	return key
}
func (e *embeddedEntry[K, V]) Value() V {
	if !e.waitPayload() {
		var zero V
		return zero
	}
	e.acquireRead()
	value := e.value
	e.releaseRead()
	return value
}
func (e *embeddedEntry[K, V]) KeyHash() (uint64, uint64) { return e.Key().KeyHash() }
func (e *embeddedEntry[K, V]) PtrMapHead() *MapHead      { return &e.MapHead }

func (e *embeddedEntry[K, V]) Offset() uintptr {
	return unsafe.Offsetof(e.MapHead) + unsafe.Offsetof(e.MapHead.ListHead)
}

func (e *embeddedEntry[K, V]) storeTypedKeyValue(key K, value V) {
	if mapState(atomic.LoadUint64((*uint64)(&e.state)))&mapPayloadState == 0 {
		e.initializePayload(key, value)
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

func (e *embeddedEntry[K, V]) loadTypedKeyValue() (K, V, mapState) {
	return e.loadKeyValue(true)
}

func (e *embeddedEntry[K, V]) loadKeyValue(withValue bool) (K, V, mapState) {
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

func (e *embeddedEntry[K, V]) waitPayload() bool {
	for {
		state := mapState(atomic.LoadUint64((*uint64)(&e.state))) & mapPayloadState
		if state != mapPayloadInitializing {
			return state == mapPayloadReady
		}
		runtime.Gosched()
	}
}

func (e *embeddedEntry[K, V]) claimPayload() bool {
	for {
		state := atomic.LoadUint64((*uint64)(&e.state))
		if mapState(state)&mapPayloadState != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64((*uint64)(&e.state), state, state|uint64(mapPayloadInitializing)) {
			return true
		}
	}
}

func (e *embeddedEntry[K, V]) isReusable() bool {
	return mapState(atomic.LoadUint64((*uint64)(&e.state)))&mapIsReusable != 0
}

func (e *embeddedEntry[K, V]) initializePayload(key K, value V) {
	if !e.claimPayload() {
		e.waitPayload()
		return
	}
	e.key, e.value = key, value
	atomic.AddUint64((*uint64)(&e.state), uint64(mapPayloadInitializing))
}

func (e *embeddedEntry[K, V]) acquireRead() {
	if !e.isReusable() {
		return
	}
	for {
		state := atomic.LoadUint64((*uint64)(&e.state))
		if mapState(state)&mapReadersMask < mapReadersMask-mapReaderUnit &&
			atomic.CompareAndSwapUint64((*uint64)(&e.state), state, state+uint64(mapReaderUnit)) {
			return
		}
		runtime.Gosched()
	}
}

func (e *embeddedEntry[K, V]) releaseRead() {
	if e.isReusable() {
		atomic.AddUint64((*uint64)(&e.state), ^uint64(mapReaderUnit-1))
	}
}

func (e *embeddedEntry[K, V]) acquireWrite() {
	if !e.isReusable() {
		return
	}
	for {
		state := atomic.LoadUint64((*uint64)(&e.state))
		if mapState(state)&mapReadersMask == 0 &&
			atomic.CompareAndSwapUint64((*uint64)(&e.state), state, state|uint64(mapReadersMask)) {
			return
		}
		runtime.Gosched()
	}
}

func (e *embeddedEntry[K, V]) releaseWrite() {
	if e.isReusable() {
		atomic.AndUint64((*uint64)(&e.state), ^uint64(mapReadersMask))
	}
}

func (e *embeddedEntry[K, V]) clearRetiredValue() {
	e.acquireWrite()
	var zero V
	e.value = zero
	e.releaseWrite()
}

func newEntryList[K Key[K], V any]() elist_head.List[embeddedEntry[K, V]] {
	var e embeddedEntry[K, V]
	return elist_head.NewList[embeddedEntry[K, V]](e.Offset())
}

// viewEntry returns the enclosing external Entry, or nil for an internal slot.
func (e *embeddedEntry[K, V]) viewEntry() *Entry[K, V] {
	if e == nil || e.isReusable() {
		return nil
	}
	return (*Entry[K, V])(unsafe.Pointer(e))
}
