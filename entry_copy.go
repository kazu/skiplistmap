package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Only NewEntryMap and tryReplaceEntry allocate this owner. Keep entryHMap's
// original size: ordinary map traversal also uses it as a MapHead view.
// A pointer to the embedded entry keeps the entire owner allocation alive.
type copyEntry struct {
	entryHMap
	root     *copyEntry
	retained atomic.Pointer[copyEntry]
	previous *copyEntry
}

// replaceEntry publishes an immutable value copy for caller-owned entries.
func (h *Map) replaceEntry(old *entryHMap, value interface{}) bool {
	stepAt("copy.update", unsafe.Pointer(old.PtrListHead()), nil)
	stepAt("update.found", unsafe.Pointer(&old.ListHead), nil)
	for {
		if old.claimBusy() {
			if !old.ListHead.IsMarked() {
				if old.IsDeleted() || old.ListHead.IsSingle() || h.tryReplaceEntry(old, value) {
					old.releaseBusy()
					return true
				}
			}
			old.releaseBusy()
		}
		runtime.Gosched()
		k, conflict := old.KeyHash()
		item, _, found := h.getItemWithBucket(k, conflict, old.key, true)
		if !found {
			// A concurrent delete won; this update does not resurrect it.
			return true
		}
		var ok bool
		old, ok = item.(*entryHMap)
		if !ok {
			return false
		}
	}
}

func (h *Map) tryReplaceEntry(old *entryHMap, value interface{}) bool {
	fresh := new(copyEntry)
	fresh.entryPayload = old.entryPayload
	fresh.value = value
	*(*[2]uint64)(unsafe.Pointer(&fresh.conflict)) = *(*[2]uint64)(unsafe.Pointer(&old.conflict))
	fresh.ListHead.Init()
	fresh.root = (*copyEntry)(unsafe.Pointer(old)).root
	fresh.state = mapIsBusy
	// Relative links do not retain allocations. The caller-owned root retains
	// every copy, including copies briefly published before a failed rollback.
	for {
		fresh.previous = fresh.root.retained.Load()
		if fresh.root.retained.CompareAndSwap(fresh.previous, fresh) {
			break
		}
	}
	if err := old.ListHead.ReplaceWith(&fresh.ListHead); err != nil {
		return false
	}
	old.Delete()
	fresh.releaseBusy()
	return true
}

func (h *Map) deleteEntry(entry *entryHMap, bucket *bucket) (MapItem, *bucket, *MapHead, bool) {
	stepAt("copy.delete", unsafe.Pointer(entry.PtrListHead()), nil)
	for {
		if !entry.ListHead.IsMarked() {
			won, busy := entry.claimDelete(mapIsBusy)
			if busy {
				return nil, nil, nil, false
			}
			if won {
				stepAt("delete.claimed", unsafe.Pointer(&entry.ListHead), nil)
				h.AddLen(-1)
				return entry, bucket, &entry.MapHead, true
			}
			if !entry.ListHead.IsMarked() {
				return nil, nil, nil, false
			}
		}
		k, conflict := entry.KeyHash()
		item, b, found := h.getItemWithBucket(k, conflict, entry.key, true)
		if !found {
			return nil, nil, nil, false
		}
		var ok bool
		entry, ok = item.(*entryHMap)
		if !ok {
			return nil, nil, nil, false
		}
		bucket = b
	}
}
