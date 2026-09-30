package skiplistmap

import (
	"bytes"
	"math/bits"
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// storeKeyValue publishes a replacement while its pool's writer owns the slot.
func (s *SampleItem) storeKeyValue(key string, value interface{}) {
	atomic.AddUint64((*uint64)(&s.state), uint64(mapKeyWriting))
	defer atomic.AddUint64((*uint64)(&s.state), uint64(mapKeyWriting))
	s.SetValue(value)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.K)), unsafe.Pointer(unsafe.StringData(key)))
	atomic.StoreUintptr((*uintptr)(unsafe.Add(unsafe.Pointer(&s.K), unsafe.Sizeof(uintptr(0)))), uintptr(len(key)))
}

// loadKeyValue reads both string words and, when requested, the value within
// one publication.
func (s *SampleItem) loadKeyValue(withValue bool) (string, interface{}, mapState) {
	for {
		state := atomic.LoadUint64((*uint64)(&s.state))
		if mapState(state)&mapKeyWriting != 0 {
			runtime.Gosched()
			continue
		}
		data := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.K)))
		if stepEnabled {
			stepAt("key.dataRead", unsafe.Pointer(s.PtrListHead()), data)
		}
		length := atomic.LoadUintptr((*uintptr)(unsafe.Add(unsafe.Pointer(&s.K), unsafe.Sizeof(uintptr(0)))))
		var value interface{}
		if withValue {
			value = s.V.Load()
		}
		if atomic.LoadUint64((*uint64)(&s.state)) == state {
			return unsafe.String((*byte)(data), length), value, mapState(state)
		}
	}
}

func equalStringKey(stored string, key interface{}) bool {
	switch key := key.(type) {
	case string:
		return stored == key
	case []byte:
		return stored == string(key)
	default:
		return false
	}
}

func equalKeys(a, b interface{}) bool {
	switch a := a.(type) {
	case string:
		return equalStringKey(a, b)
	case []byte:
		switch b := b.(type) {
		case string:
			return string(a) == b
		case []byte:
			return bytes.Equal(a, b)
		}
		return false
	case uint64, byte, int, int32, uint32, int64:
		switch b.(type) {
		case uint64, byte, int, int32, uint32, int64:
			ka, _ := KeyToHash(a)
			kb, _ := KeyToHash(b)
			return ka == kb
		}
		return false
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.ValueOf(a).Comparable() {
		return a == b
	}
	// KeyToHash does not support other non-comparable keys. Caller-defined
	// hash-only items retain their existing hash-pair identity for those keys.
	return true
}

func equalItemKey(item MapItem, key interface{}) bool {
	if sample, ok := item.(*SampleItem); ok {
		stored, _, _ := sample.loadKeyValue(false)
		return equalStringKey(stored, key)
	}
	return equalKeys(item.Key(), key)
}

func (h *Map) getValueMatching(hash, conflict uint64, key interface{}, byKey bool) (interface{}, bool) {
	e := h.searchItem(hash)
	for e != nil {
		if stepEnabled {
			stepAt("get.beforeValue", unsafe.Pointer(e.PtrListHead()), nil)
		}
		if value, ok := readMatchingEntry(e, bits.Reverse64(hash), conflict, key, byKey, true, h.isEmbededItemInBucket); ok {
			return value, true
		}
		item, found := h.getItemMatching(hash, conflict, key, byKey)
		if !found {
			return nil, false
		}
		e = item
	}
	return nil, false
}

func readMatchingEntry(e HMapEntry, reverse, conflict uint64, key interface{}, byKey, withValue, reusable bool) (interface{}, bool) {
	var value interface{}
	var state mapState
	sample, isSample := e.(*SampleItem)
	var mh *MapHead
	if isSample {
		mh = &sample.MapHead
	} else {
		mh = e.PtrMapHead()
	}
	snapshot := isSample && (byKey || withValue)
	if snapshot {
		var stored string
		if reusable {
			stored, value, state = sample.loadKeyValue(withValue)
		} else {
			// Non-embedded pools never reuse a published slot for another key.
			stored = sample.K
			if withValue {
				value = sample.V.Load()
			}
			state = mapState(atomic.LoadUint64((*uint64)(&sample.state)))
		}
		if state&(mapIsDummy|mapIsDeleted) != 0 || (byKey && !equalStringKey(stored, key)) {
			return nil, false
		}
	} else {
		if mh.IsIgnored() || (byKey && !equalKeys(e.(MapItem).Key(), key)) {
			return nil, false
		}
		if withValue {
			value = e.(MapItem).Value()
		}
	}
	if !matchesHash(mh, reverse, conflict) {
		return nil, false
	}
	// A value-only update can change busy without changing the key identity.
	if snapshot && reusable && mapState(atomic.LoadUint64((*uint64)(&mh.state)))&^mapIsBusy != state&^mapIsBusy {
		return nil, false
	}
	return value, true
}

func matchesLiveHash(mh *MapHead, reverse, conflict uint64) bool {
	return !mh.IsIgnored() && matchesHash(mh, reverse, conflict)
}

func matchesHash(mh *MapHead, reverse, conflict uint64) bool {
	return atomic.LoadUint64(&mh.reverse) == reverse && atomic.LoadUint64(&mh.conflict) == conflict &&
		linkedEntry(mh.PtrListHead())
}

func linkedEntry(head *elist_head.ListHead) bool {
	return head.DirectNext() != head && head.DirectPrev() != head
}
