package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// replaceEntry publishes an immutable value copy for nonembedded entries.
func (h *Map[K, V]) replaceEntry(old *Entry[K, V], value V) bool {
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
		hash, conflict := old.KeyHash()
		item, _, found := h.getItemWithBucket(hash, conflict, old.key, true)
		if !found {
			// A concurrent delete won; this update does not resurrect it.
			return true
		}
		old = item.viewEntry()
	}
}

func (h *Map[K, V]) tryReplaceEntry(old *Entry[K, V], value V) bool {
	fresh := NewEntry(old.Key(), value)
	prepareEntryReplacement(old, fresh)
	return publishEntryReplacement(old, fresh)
}

func prepareEntryReplacement[K Key[K], V any](old, fresh *Entry[K, V]) {
	*(*[2]uint64)(unsafe.Pointer(&fresh.conflict)) = *(*[2]uint64)(unsafe.Pointer(&old.conflict))
	fresh.ListHead.Init()
	fresh.state = mapIsBusy | mapPayloadReady | (mapState(atomic.LoadUint64((*uint64)(&old.state))) & mapIsPoolItem)
	fresh.retainedEntry = old.retainedEntry
	old.retainedEntry = fresh
}

func publishEntryReplacement[K Key[K], V any](old, fresh *Entry[K, V]) bool {
	_, publishedEntry, published := replaceEntryListNode(&old.embeddedEntry, &fresh.embeddedEntry, "copy.replacement.inserted")
	if !published {
		return false
	}
	publishedEntry.releaseBusy()
	return true
}

func replaceEntryListNode[K Key[K], V any](old, fresh *embeddedEntry[K, V], point string) (*embeddedEntry[K, V], *embeddedEntry[K, V], bool) {
	for {
		oldHead := movedHead(&old.ListHead)
		freshHead := movedHead(&fresh.ListHead)
		old = entryHMapFromListHead[K, V](oldHead)
		fresh = entryHMapFromListHead[K, V](freshHead)
		if _, err := oldHead.InsertBefore(freshHead); err != nil {
			if elist_head.IsMoved(oldHead) {
				old = entryHMapFromListHead[K, V](movedHead(oldHead))
			}
			if oldHead.IsMarked() && !elist_head.IsMoved(oldHead) {
				return old, fresh, false
			}
			runtime.Gosched()
			continue
		}
		stepAt(point, unsafe.Pointer(freshHead), unsafe.Pointer(oldHead))
		for {
			if err := oldHead.MarkForDelete(); err == nil {
				old = entryHMapFromListHead[K, V](oldHead)
				stepAt(point+".marked", unsafe.Pointer(freshHead), unsafe.Pointer(oldHead))
				old.Delete()
				return old, entryHMapFromListHead[K, V](movedHead(freshHead)), true
			}
			if elist_head.IsMoved(oldHead) {
				oldHead = movedHead(oldHead)
				old = entryHMapFromListHead[K, V](oldHead)
				continue
			}
			if oldHead.IsMarked() {
				old = entryHMapFromListHead[K, V](oldHead)
				old.Delete()
				return old, entryHMapFromListHead[K, V](movedHead(freshHead)), true
			}
			runtime.Gosched()
		}
	}
}

func (h *Map[K, V]) deleteEntry(entry *entryHMap[K, V], bucket *bucket[K, V], dummy *MapHead) (*embeddedEntry[K, V], *bucket[K, V], *MapHead, bool) {
	stepAt("copy.delete", unsafe.Pointer(entry.PtrListHead()), nil)
	for {
		if !entry.ListHead.IsMarked() {
			hold := mapIsDeleted | mapIsBusy
			if !entry.isPoolItem() {
				hold = mapIsBusy
			}
			won, busy := entry.claimLive(hold)
			if busy {
				if !entry.isPoolItem() {
					// Preserve StoreItem's refusal while a caller-owned entry
					// is still being published.
					return nil, nil, nil, false
				}
				// A Set update does not make an existing pool key absent.
				// Wait for its replacement, then retry on the current entry.
				runtime.Gosched()
				continue
			}
			if won {
				if !entry.isPoolItem() {
					if dummy == nil {
						dummy = mapheadFromLListHead(bucket.head())
					}
					if !entry.reachableFromDummy(&dummy.ListHead) {
						entry.releaseBusy()
						return nil, nil, nil, false
					}
					atomic.OrUint64((*uint64)(&entry.state), uint64(mapIsDeleted|mapIsRetired))
				}
				stepAt("delete.claimed", unsafe.Pointer(&entry.ListHead), nil)
				h.AddLen(-1)
				return entry, bucket, &entry.MapHead, true
			}
			if !entry.ListHead.IsMarked() {
				return nil, nil, nil, false
			}
		}
		k, conflict := entry.KeyHash()
		item, b, d, found := h.lookupItem[saveDummyTrace](k, conflict, entry.key, true)
		if !found {
			return nil, nil, nil, false
		}
		entry = item
		bucket = b
		dummy = d
	}
}

// reachableFromDummy runs with the caller-owned entry busy and retries from
// the saved dummy if traversal reaches a purged neighbor.
func (entry *embeddedEntry[K, V]) reachableFromDummy(dummy *elist_head.ListHead) bool {
	reverse := atomic.LoadUint64(&entry.reverse)
	startReverse := atomic.LoadUint64(&mapheadFromLListHead(dummy).reverse)
	forward := startReverse < reverse
	for {
		cur := dummy
		for {
			stepAt("delete.scan", unsafe.Pointer(cur), unsafe.Pointer(&entry.ListHead))
			var next *elist_head.ListHead
			if forward {
				next = elist_head.NextNoM(cur)
			} else {
				next = elist_head.PrevNoM(cur)
			}
			if next == cur {
				// A neighbor may have been purged after we reached it.
				cur = dummy
				runtime.Gosched()
				continue
			}
			if next.Empty() {
				break
			}
			if next == &entry.ListHead {
				return true
			}
			r := atomic.LoadUint64(&mapheadFromLListHead(next).reverse)
			if forward && r > reverse || !forward && r < reverse {
				break
			}
			cur = next
		}
		if !forward && startReverse == reverse {
			forward = true
			continue
		}
		return false
	}
}

// replacePoolEntry runs while Set owns the base bucket's muPool. It publishes
// another array slot before retiring the old one; only slot reclamation waits
// for readers of the old payload.
func (h *Map[K, V]) replacePoolEntry(old *embeddedEntry[K, V], value V) bool {
	old, fresh, found := h.preparePoolReplacement(old, value)
	if !found {
		return false
	}
	return publishPoolReplacement(old, fresh)
}

func (h *Map[K, V]) preparePoolReplacement(old *embeddedEntry[K, V], value V) (*embeddedEntry[K, V], *embeddedEntry[K, V], bool) {
	key := old.Key()
	hash, conflict := key.KeyHash()
	// read before the slot is allocated: the allocation may rebuild the
	// array of old and let the old array go to the free list, cleared
	reverse := atomic.LoadUint64(&old.reverse)
	owner := h.findBucket(reverse).toBase()
	pool := owner.itemPool()
	fresh, nextPool, _ := pool.getWithFn(reverse, nil, &h.free)
	if nextPool != nil {
		owner.setItemPool(nextPool)
	}
	fresh.storeTypedKeyValue(key, value)
	atomic.StoreUint64(&fresh.reverse, reverse)
	atomic.StoreUint64(&fresh.conflict, conflict)
	atomic.AndUint64((*uint64)(&fresh.state), ^uint64(mapIsDeleted|mapIsDummy))
	atomic.OrUint64((*uint64)(&fresh.state), uint64(mapIsPoolItem|mapIsBusy))
	fresh.ListHead.Init()
	// Allocating the slot may have moved the old array.
	current, _, found := h.getItemWithBucket(hash, conflict, key, true)
	if !found {
		fresh.Delete()
		fresh.releaseBusy()
		fresh.clearRetiredValue()
		return nil, nil, false
	}
	old = current
	stepAt("update.found", unsafe.Pointer(old.PtrListHead()), nil)
	return old, fresh, true
}

func publishPoolReplacement[K Key[K], V any](old, fresh *embeddedEntry[K, V]) bool {
	for {
		var published bool
		old, fresh, published = replaceEntryListNode(old, fresh, "copy.replacement.inserted")
		if published {
			break
		}
		runtime.Gosched()
	}
	old.ListHead.Init()
	old.clearRetiredValue()
	fresh.releaseBusy()
	return true
}
