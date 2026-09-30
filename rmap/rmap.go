package rmap

import (
	"reflect"
	"sync"
	"sync/atomic"

	smap "github.com/kazu/skiplistmap"
)

type baseMap struct {
	sync.RWMutex
	read        atomic.Pointer[readMap]
	onNewStores []func(smap.MapItem[smap.StringKey, any])
}

type RMap struct {
	baseMap
	// The version detects changes back to the same count. Active changes cover
	// the interval between publishing a deletion/revival and updating the count.
	len          atomic.Int64
	countVersion atomic.Uint64
	countChanges atomic.Int64
}

// A generation owns one dirty map. Once closed, its structure is immutable;
// shared readSlot values can still be updated through the next generation.
type mapGeneration struct {
	dirty  *smap.Map[smap.StringKey, *readSlot]
	next   atomic.Pointer[readMap]
	misses atomic.Uint64
	phase  atomic.Uint32
	users  atomic.Int64
}

const (
	generationClosing = 1
	generationClosed  = 2
)

type readMap struct {
	generation *mapGeneration
	m          map[uint64]*readSlot
	collisions map[uint64][]*readSlot
	frozen     *smap.Map[smap.StringKey, *readSlot]
}

type readSlot struct {
	key      string
	conflict uint64
	value    atomic.Pointer[storedValue]
}

type storedValue struct {
	data atomic.Value
}

func newStoredValue(v interface{}) *storedValue {
	value := &storedValue{}
	if v != nil {
		value.data.Store(v)
	}
	return value
}

var deleted = &storedValue{}

// An expunged slot cannot be revived: promotion omitted it, so Set must insert
// the key into the current dirty map instead.
var expunged = &storedValue{}

func New() *RMap {
	m := &RMap{}
	g := &mapGeneration{dirty: newDirty()}
	m.read.Store(&readMap{generation: g})
	return m
}

func newDirty() *smap.Map[smap.StringKey, *readSlot] {
	return smap.New[smap.StringKey, *readSlot](
		smap.UsePool[smap.StringKey, *readSlot](true),
		smap.BucketMode[smap.StringKey, *readSlot](smap.CombineSearch4),
		smap.MaxPefBucket[smap.StringKey, *readSlot](16),
	)
}

func (m *RMap) acquireDirty() *readMap {
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

func loadDirtySlot(dirty *smap.Map[smap.StringKey, *readSlot], k, conflict uint64, key string, full bool) *readSlot {
	item, ok := dirty.GetByHash(k, conflict)
	if ok && full && item.key != key {
		item, ok = dirty.Get(smap.StringKey(key))
	}
	if !ok {
		return nil
	}
	return item
}

func (r *readMap) loadSlot(k, conflict uint64, key string, full bool) *atomic.Pointer[storedValue] {
	if slot := r.m[k]; slot != nil && slot.conflict == conflict && (!full || slot.key == key) {
		return &slot.value
	}
	for _, slot := range r.collisions[k] {
		if slot.conflict == conflict && (!full || slot.key == key) {
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

func (r *readMap) store2(m *RMap, k, conflict uint64, key string, v interface{}) bool {
	slot := r.loadSlot(k, conflict, key, true)
	if slot == nil {
		return false
	}
	return storeValue(slot, v, m)
}

func storeValue(slot *atomic.Pointer[storedValue], v interface{}, counter *RMap) bool {
	for {
		old := slot.Load()
		if old == expunged || (old == deleted && counter == nil) {
			return false
		}
		stepAt("value.loaded")
		// Each cell retains its concrete type. A type change or revival gets a
		// new cell; an update still holding a detached cell precedes its removal.
		if old != deleted && reflect.TypeOf(old.data.Load()) == reflect.TypeOf(v) {
			if v != nil {
				old.data.Store(v)
			}
			return true
		}
		next := newStoredValue(v)
		if old == deleted {
			counter.countChanges.Add(1)
		}
		ok := slot.CompareAndSwap(old, next)
		if old == deleted {
			if ok {
				counter.addReadLen(1)
			}
			counter.countChanges.Add(-1)
		}
		if ok {
			return true
		}
	}
}

func (m *RMap) Set(key string, v interface{}) bool {
	k, conflict := smap.KeyToHash(key)
	return m.Set2(k, conflict, key, v)
}

func (m *RMap) Set2(k, conflict uint64, key string, v interface{}) bool {
	r := m.read.Load()
	ok := r.store2(m, k, conflict, key, v)
	if !ok {
		// Existing value cells are shared across promotion. Only structural
		// changes need to pin dirty; never revive a deleted cell in this path.
		if slot := loadDirtySlot(r.generation.dirty, k, conflict, key, true); slot != nil {
			if storeValue(&slot.value, v, nil) {
				return true
			}
		}
		r = m.acquireDirty()
		g := r.generation
		ok = r.store2(m, k, conflict, key, v)
		if !ok {
			if slot := loadDirtySlot(g.dirty, k, conflict, key, true); slot != nil {
				ok = storeValue(&slot.value, v, nil)
			} else {
				stepAt("set.dirtyMissing")
				slot := &readSlot{key: key, conflict: conflict}
				slot.value.Store(newStoredValue(v))
				ok = g.dirty.Set(smap.StringKey(key), slot)
			}
			g.users.Add(-1)
			return ok
		}
		g.users.Add(-1)
	}
	if len(m.onNewStores) != 0 {
		item := smap.NewSampleItem(smap.StringKey(key), v)
		item.PtrListHead().Init()
		item.Setup()
		for _, fn := range m.onNewStores {
			fn(item)
		}
	}
	return true
}

func (m *RMap) Get(key string) (interface{}, bool) {
	k, conflict := smap.KeyToHash(key)
	return m.get(k, conflict, key, true)
}

func (m *RMap) Get2(k, conflict uint64) (interface{}, bool) {
	return m.get(k, conflict, "", false)
}

func (m *RMap) get(k, conflict uint64, key string, full bool) (interface{}, bool) {
	r := m.read.Load()
	if slot := r.loadSlot(k, conflict, key, full); slot != nil {
		v := slot.Load()
		if v == deleted {
			return nil, false
		}
		if v != expunged {
			return v.data.Load(), true
		}
	}
	g := r.generation
	slot := loadDirtySlot(g.dirty, k, conflict, key, full)
	m.missLocked(g)
	if slot != nil {
		v := slot.value.Load()
		if v != deleted && v != expunged {
			return v.data.Load(), true
		}
	}
	return nil, false
}

func (m *RMap) deleteRead(r *readMap, k, conflict uint64, key string) (bool, bool) {
	slot := r.loadSlot(k, conflict, key, true)
	if slot == nil {
		return false, false
	}
	for {
		old := slot.Load()
		if old == expunged {
			return false, false
		}
		if old == deleted {
			return false, true
		}
		m.countChanges.Add(1)
		ok := slot.CompareAndSwap(old, deleted)
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

func (m *RMap) Delete(key string) bool {
	k, conflict := smap.KeyToHash(key)
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
	ok := g.dirty.Delete(smap.StringKey(key))
	g.users.Add(-1)
	m.missLocked(g)
	return ok
}

func (m *RMap) Len() int {
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

func (m *RMap) addReadLen(delta int64) {
	if delta == 0 {
		return
	}
	m.len.Add(delta)
	m.countVersion.Add(1)
}

func (m *RMap) missLocked(g *mapGeneration) {
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

func (r *readMap) addSlot(k uint64, slot *readSlot) {
	for {
		v := slot.value.Load()
		if v == expunged {
			return
		}
		if v == deleted {
			if slot.value.CompareAndSwap(deleted, expunged) {
				return
			}
			continue
		}
		if r.m[k] == nil {
			r.m[k] = slot
		} else {
			if r.collisions == nil {
				r.collisions = make(map[uint64][]*readSlot)
			}
			r.collisions[k] = append(r.collisions[k], slot)
		}
		return
	}
}

func (m *RMap) promote(g *mapGeneration) {
	if !m.TryLock() {
		return
	}
	defer m.Unlock()
	old := m.read.Load()
	if old.generation != g || g.users.Load() != 0 {
		return
	}
	next := &mapGeneration{dirty: newDirty()}
	// Publish a view of both sources before copying. Writers may publish next
	// after closing, and every view must share the same value cells.
	pending := &readMap{generation: next, m: old.m, collisions: old.collisions, frozen: g.dirty}
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
	read := &readMap{generation: next, m: make(map[uint64]*readSlot, len(old.m)+int(g.dirty.Len()))}
	for k, slot := range old.m {
		read.addSlot(k, slot)
	}
	for k, slots := range old.collisions {
		for _, slot := range slots {
			read.addSlot(k, slot)
		}
	}
	g.dirty.RangeItem(func(item smap.MapItem[smap.StringKey, *readSlot]) bool {
		e := item
		k, _ := e.KeyHash()
		read.addSlot(k, e.Value())
		return true
	})
	stepAt("promote.copied")
	m.read.Store(read)
}
