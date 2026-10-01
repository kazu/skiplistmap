package skiplistmap

import (
	"runtime"
	"unsafe"

	"github.com/lk4d4/trylock"
)

// Update calls edit once for an existing key and returns true after publishing
// the edited value. For an absent key it returns false without calling edit.
// A nil edit panics, including when the key is absent.
//
// The value pointer is valid only during edit: do not retain it or pass it to
// another goroutine. Do not reenter the same Map from edit. Referents of slices,
// maps and pointers in V retain their existing ownership and synchronization
// requirements. If edit panics, protection is released; rollback is not promised.
func (h *Map[K, V]) Update(key K, edit func(*V)) bool {
	if edit == nil {
		panic("skiplistmap: nil Update callback")
	}
	hash, conflict := key.KeyHash()
	for {
		item, bucket, found := h.getItemWithBucket(hash, conflict, key, true)
		if !found {
			return false
		}
		if stepEnabled {
			stepAt("update.lookup", unsafe.Pointer(item.PtrListHead()), unsafe.Pointer(bucket))
		}
		var mu *trylock.Mutex
		if h.isEmbededItemInBucket {
			var valid bool
			mu, valid = h.lockFoundItem(key, item, bucket)
			if !valid {
				runtime.Gosched()
				continue
			}
			if item.isPoolItem() {
				defer mu.Unlock()
				return h.editPoolEntry(item, edit)
			}
		}
		if item.claimBusy() {
			if !item.IsDeleted() && !item.ListHead.IsMarked() && !item.ListHead.IsSingle() {
				defer item.releaseBusy()
				if mu != nil {
					defer mu.Unlock()
				}
				fresh := NewEntry(item.key, item.value)
				edit(&fresh.value)
				prepareEntryReplacement(item, fresh)
				for !publishEntryReplacement(item, fresh) {
					runtime.Gosched()
				}
				return true
			}
			item.releaseBusy()
		}
		if mu != nil {
			mu.Unlock()
		}
		runtime.Gosched()
	}
}

func (h *Map[K, V]) editPoolEntry(old *Entry[K, V], edit func(*V)) bool {
	old, fresh, found := h.preparePoolReplacement(old, old.value)
	if !found {
		return false
	}
	published := false
	fresh.acquireWrite()
	defer func() {
		fresh.releaseWrite()
		if !published {
			fresh.Delete()
			fresh.releaseBusy()
			fresh.clearRetiredValue()
		}
	}()
	edit(&fresh.value)
	published = publishPoolReplacement(old, fresh)
	return published
}
