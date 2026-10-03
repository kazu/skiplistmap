package rmap

import (
	"sync"
	"sync/atomic"

	smap "github.com/kazu/skiplistmap"
)

type baseMap[K smap.Key[K], V any] struct {
	sync.RWMutex
	read        atomic.Pointer[readMap[K, V]]
	onNewStores []func(smap.MapItem[K, V])
}

type RMap[K smap.Key[K], V any] struct {
	baseMap[K, V]
	// The version detects changes back to the same count. Active changes cover
	// the interval between publishing a deletion/revival and updating the count.
	len          atomic.Int64
	countVersion atomic.Uint64
	countChanges atomic.Int64
}

// A generation owns one dirty map. Once closed, its structure is immutable;
// shared readSlot[K, V] values can still be updated through the next generation.
type mapGeneration[K smap.Key[K], V any] struct {
	dirty  *smap.Map[K, *readSlot[K, V]]
	next   atomic.Pointer[readMap[K, V]]
	misses atomic.Uint64
	phase  atomic.Uint32
	users  atomic.Int64
}

const (
	generationClosing = 1
	generationClosed  = 2
)

type readMap[K smap.Key[K], V any] struct {
	generation *mapGeneration[K, V]
	m          map[uint64]*readSlot[K, V]
	collisions map[uint64][]*readSlot[K, V]
	frozen     *smap.Map[K, *readSlot[K, V]]
}

type readSlot[K smap.Key[K], V any] struct {
	key      K
	conflict uint64
	// A nil cell is deleted. A cell with nil data was omitted by promotion.
	value atomic.Pointer[storedValue[V]]
}

type storedValue[V any] struct {
	data atomic.Pointer[V]
}

func newStoredValue[V any](v *V) *storedValue[V] {
	value := &storedValue[V]{}
	value.data.Store(v)
	return value
}

// New creates a map with typed read and dirty generations.
func New[K smap.Key[K], V any]() *RMap[K, V] {
	m := &RMap[K, V]{}
	g := &mapGeneration[K, V]{dirty: newDirty[K, V]()}
	m.read.Store(&readMap[K, V]{generation: g})
	return m
}

func newDirty[K smap.Key[K], V any]() *smap.Map[K, *readSlot[K, V]] {
	return smap.New[K, *readSlot[K, V]](
		smap.UsePool[K, *readSlot[K, V]](true),
		smap.BucketMode[K, *readSlot[K, V]](smap.CombineSearch4),
		smap.MaxPefBucket[K, *readSlot[K, V]](16),
	)
}

func (m *RMap[K, V]) acquireDirty() *readMap[K, V] {
	for {
		r := m.read.Load()
		g := r.generation
		g.users.Add(1)
		phase := g.phase.Load()
		if phase == 0 {
			return r
		}
		g.users.Add(-1)
		if phase == generationClosed {
			// A paused promoter must not prevent a writer from making progress.
			m.read.CompareAndSwap(r, g.next.Load())
		} else {
			g.phase.CompareAndSwap(generationClosing, 0)
		}
	}
}

func loadDirtySlot[K smap.Key[K], V any](dirty *smap.Map[K, *readSlot[K, V]], k, conflict uint64, key K, full bool) *readSlot[K, V] {
	item, ok := dirty.GetByHash(k, conflict)
	if ok && full && !item.key.Equal(key) {
		item, ok = dirty.Get(key)
	}
	if !ok {
		return nil
	}
	return item
}

func (r *readMap[K, V]) loadSlot(k, conflict uint64, key K, full bool) *atomic.Pointer[storedValue[V]] {
	if slot := r.m[k]; slot != nil && slot.conflict == conflict && (!full || slot.key.Equal(key)) {
		return &slot.value
	}
	for _, slot := range r.collisions[k] {
		if slot.conflict == conflict && (!full || slot.key.Equal(key)) {
			return &slot.value
		}
	}
	if r.frozen != nil {
		if slot := loadDirtySlot(r.frozen, k, conflict, key, full); slot != nil {
			return &slot.value
		}
	}
	return nil
}

func (r *readMap[K, V]) store2(m *RMap[K, V], k, conflict uint64, key K, v *V) bool {
	slot := r.loadSlot(k, conflict, key, true)
	if slot == nil {
		return false
	}
	return storeValue[K, V](slot, v, m)
}

func storeValue[K smap.Key[K], V any](slot *atomic.Pointer[storedValue[V]], v *V, counter *RMap[K, V]) bool {
	for {
		old := slot.Load()
		if (old != nil && old.data.Load() == nil) || (old == nil && counter == nil) {
			return false
		}
		stepAt("value.loaded")
		// An update holding a detached cell precedes its removal.
		if old != nil {
			old.data.Store(v)
			return true
		}
		next := newStoredValue(v)
		counter.countChanges.Add(1)
		ok := slot.CompareAndSwap(nil, next)
		if ok {
			counter.addReadLen(1)
		}
		counter.countChanges.Add(-1)
		if ok {
			return true
		}
	}
}

func (m *RMap[K, V]) Set(key K, v V) bool {
	k, conflict := key.KeyHash()
	return m.Set2(k, conflict, key, v)
}

func (m *RMap[K, V]) Set2(k, conflict uint64, key K, v V) bool {
	r := m.read.Load()
	ok := r.store2(m, k, conflict, key, &v)
	if !ok {
		// Existing value cells are shared across promotion. Only structural
		// changes need to pin dirty; never revive a deleted cell in this path.
		if slot := loadDirtySlot(r.generation.dirty, k, conflict, key, true); slot != nil {
			if storeValue[K, V](&slot.value, &v, nil) {
				return true
			}
		}
		r = m.acquireDirty()
		g := r.generation
		ok = r.store2(m, k, conflict, key, &v)
		if !ok {
			if slot := loadDirtySlot(g.dirty, k, conflict, key, true); slot != nil {
				ok = storeValue[K, V](&slot.value, &v, nil)
			} else {
				stepAt("set.dirtyMissing")
				slot := &readSlot[K, V]{key: key, conflict: conflict}
				slot.value.Store(newStoredValue(&v))
				ok = g.dirty.Set(key, slot)
			}
			g.users.Add(-1)
			return ok
		}
		g.users.Add(-1)
	}
	if len(m.onNewStores) != 0 {
		item := smap.NewSampleItem(key, v)
		item.PtrListHead().Init()
		item.Setup()
		for _, fn := range m.onNewStores {
			fn(item)
		}
	}
	return true
}

// Get returns the value for key, or the zero value of V and false if absent.
func (m *RMap[K, V]) Get(key K) (V, bool) {
	k, conflict := key.KeyHash()
	return m.get(k, conflict, key, true)
}

func (m *RMap[K, V]) Get2(k, conflict uint64) (V, bool) {
	var key K
	return m.get(k, conflict, key, false)
}

func (m *RMap[K, V]) get(k, conflict uint64, key K, full bool) (V, bool) {
	r := m.read.Load()
	if slot := r.loadSlot(k, conflict, key, full); slot != nil {
		v := slot.Load()
		if v == nil {
			var zero V
			return zero, false
		}
		if value := v.data.Load(); value != nil {
			return *value, true
		}
	}
	g := r.generation
	slot := loadDirtySlot(g.dirty, k, conflict, key, full)
	m.missLocked(g)
	if slot != nil {
		v := slot.value.Load()
		if v != nil {
			if value := v.data.Load(); value != nil {
				return *value, true
			}
		}
	}
	var zero V
	return zero, false
}

func (m *RMap[K, V]) deleteRead(r *readMap[K, V], k, conflict uint64, key K) (bool, bool) {
	slot := r.loadSlot(k, conflict, key, true)
	if slot == nil {
		return false, false
	}
	for {
		old := slot.Load()
		if old != nil && old.data.Load() == nil {
			return false, false
		}
		if old == nil {
			return false, true
		}
		m.countChanges.Add(1)
		ok := slot.CompareAndSwap(old, nil)
		if ok {
			stepAt("delete.beforeCount")
			m.addReadLen(-1)
		}
		m.countChanges.Add(-1)
		if ok {
			return true, true
		}
	}
}

func (m *RMap[K, V]) Delete(key K) bool {
	k, conflict := key.KeyHash()
	r := m.read.Load()
	if ok, found := m.deleteRead(r, k, conflict, key); found {
		return ok
	}
	r = m.acquireDirty()
	g := r.generation
	if ok, found := m.deleteRead(r, k, conflict, key); found {
		g.users.Add(-1)
		return ok
	}
	ok := g.dirty.Delete(key)
	g.users.Add(-1)
	m.missLocked(g)
	return ok
}

func (m *RMap[K, V]) Len() int {
	m.RLock()
	defer m.RUnlock()
	for {
		version := m.countVersion.Load()
		n := m.len.Load()
		stepAt("len.readCount")
		if m.countChanges.Load() != 0 {
			continue
		}
		dirty := int(m.read.Load().generation.dirty.Len())
		stepAt("len.dirtyCount")
		if m.countChanges.Load() == 0 && m.countVersion.Load() == version {
			return int(n) + dirty
		}
	}
}

func (m *RMap[K, V]) addReadLen(delta int64) {
	if delta == 0 {
		return
	}
	m.len.Add(delta)
	m.countVersion.Add(1)
}

func (m *RMap[K, V]) missLocked(g *mapGeneration[K, V]) {
	r := m.read.Load()
	if r.generation != g {
		return
	}
	n := g.dirty.Len()
	if n == 0 {
		return
	}
	misses := g.misses.Add(1)
	if misses <= uint64(len(r.m)) && misses < uint64(n) {
		return
	}
	m.promote(g)
}

func (r *readMap[K, V]) addSlot(k uint64, slot *readSlot[K, V]) {
	for {
		v := slot.value.Load()
		if v != nil && v.data.Load() == nil {
			return
		}
		if v == nil {
			if slot.value.CompareAndSwap(nil, &storedValue[V]{}) {
				return
			}
			continue
		}
		if r.m[k] == nil {
			r.m[k] = slot
		} else {
			if r.collisions == nil {
				r.collisions = make(map[uint64][]*readSlot[K, V])
			}
			r.collisions[k] = append(r.collisions[k], slot)
		}
		return
	}
}

func (m *RMap[K, V]) promote(g *mapGeneration[K, V]) {
	if !m.TryLock() {
		return
	}
	defer m.Unlock()
	old := m.read.Load()
	if old.generation != g || g.users.Load() != 0 {
		return
	}
	next := &mapGeneration[K, V]{dirty: newDirty[K, V]()}
	// Publish a view of both sources before copying. Writers may publish next
	// after closing, and every view must share the same value cells.
	pending := &readMap[K, V]{generation: next, m: old.m, collisions: old.collisions, frozen: g.dirty}
	g.next.Store(pending)
	g.phase.Store(generationClosing)
	stepAt("promote.closing")
	// A writer either entered before closing (users), or cancels closing and
	// retries. Only a closed generation may be copied without a structural race.
	if g.users.Load() != 0 || !g.phase.CompareAndSwap(generationClosing, generationClosed) {
		g.phase.CompareAndSwap(generationClosing, 0)
		g.next.Store(nil)
		return
	}
	stepAt("promote.closed")
	m.read.CompareAndSwap(old, pending)
	m.addReadLen(int64(g.dirty.Len()))
	read := &readMap[K, V]{generation: next, m: make(map[uint64]*readSlot[K, V], len(old.m)+int(g.dirty.Len()))}
	for k, slot := range old.m {
		read.addSlot(k, slot)
	}
	for k, slots := range old.collisions {
		for _, slot := range slots {
			read.addSlot(k, slot)
		}
	}
	g.dirty.RangeItem(func(item smap.MapItem[K, *readSlot[K, V]]) bool {
		e := item
		k, _ := e.KeyHash()
		read.addSlot(k, e.Value())
		return true
	})
	stepAt("promote.copied")
	m.read.Store(read)
}
