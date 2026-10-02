package skiplistmap

import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/kazu/elist_head"
)

// replaceEntry publishes an immutable value copy for nonembedded entries.
func (h *Map[K, V]) replaceEntry(old *entryHMap[K, V], value V) bool {
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
		old = item
	}
}

func (h *Map[K, V]) tryReplaceEntry(old *entryHMap[K, V], value V) bool {
	fresh := NewEntry(old.Key(), value)
	prepareEntryReplacement(old, fresh)
	return publishEntryReplacement(old, fresh)
}

func prepareEntryReplacement[K Key[K], V any](old, fresh *Entry[K, V]) {
	*(*[2]uint64)(unsafe.Pointer(&fresh.conflict)) = *(*[2]uint64)(unsafe.Pointer(&old.conflict))
	fresh.ListHead.Init()
	fresh.root = old.root
	fresh.state = mapIsBusy | (mapState(atomic.LoadUint64((*uint64)(&old.state))) & mapIsPoolItem)
	// Relative links do not retain allocations. The entry's root retains
	// every copy, including copies briefly published before a failed rollback.
	for {
		fresh.previous = fresh.root.retained.Load()
		if fresh.root.retained.CompareAndSwap(fresh.previous, fresh) {
			break
		}
	}
}

func publishEntryReplacement[K Key[K], V any](old, fresh *Entry[K, V]) bool {
	if err := old.ListHead.ReplaceWith(&fresh.ListHead); err != nil {
		return false
	}
	old.Delete()
	fresh.releaseBusy()
	return true
}

func (h *Map[K, V]) deleteEntry(entry *entryHMap[K, V], bucket *bucket[K, V], dummy *MapHead) (MapItem[K, V], *bucket[K, V], *MapHead, bool) {
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

// reachableFromDummy runs with the caller-owned entry busy. The bucket dummy
// remains in the original map even if the entry was purged and reused elsewhere.
func (entry *Entry[K, V]) reachableFromDummy(dummy *elist_head.ListHead) bool {
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
func (h *Map[K, V]) replacePoolEntry(old *Entry[K, V], value V) bool {
	old, fresh, found := h.preparePoolReplacement(old, value)
	if !found {
		return false
	}
	return publishPoolReplacement(old, fresh)
}

func (h *Map[K, V]) preparePoolReplacement(old *Entry[K, V], value V) (*Entry[K, V], *Entry[K, V], bool) {
	key := old.Key()
	hash, conflict := key.KeyHash()
	owner := h.findBucket(old.reverse).toBase()
	pool := owner.itemPool()
	fresh, nextPool, _ := pool.getWithFn(old.reverse, nil)
	if nextPool != nil {
		owner.setItemPool(nextPool)
	}
	fresh.storeTypedKeyValue(key, value)
	atomic.StoreUint64(&fresh.reverse, old.reverse)
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

func publishPoolReplacement[K Key[K], V any](old, fresh *Entry[K, V]) bool {
	for {
		if err := old.ListHead.ReplaceWith(&fresh.ListHead); err == nil {
			break
		}
		runtime.Gosched()
	}
	old.Delete()
	old.ListHead.Init()
	old.clearRetiredValue()
	fresh.releaseBusy()
	return true
}
