package rmap

import (
	"sync"
	"sync/atomic"

	"github.com/kazu/skiplistmap"
	smap "github.com/kazu/skiplistmap"
)

type baseMap[K, V any] struct {
	sync.RWMutex
	read        atomic.Value
	onNewStores []func(smap.MapItem[K, V])

	misses int
}

type RMap[K, V any] struct {
	baseMap[K, V]
	limit int
	dirty *smap.Map[K, V]
}

type readMap[K, V any] struct {
	m       map[uint64]atomic.Value
	amended bool // include dirty map
}

func New[K, V any]() (rmap *RMap[K, V]) {

	rmap = &RMap[K, V]{
		limit: 1000,
	}
	rmap.initDirty()

	return rmap

}

// func MutexFn(head *list_head.ListHead) *sync.RWMutex {
// 	e := (*entryRmap)(list_head.ElementOf(&entryRmap{}, head))
// 	if e == nil {
// 		_ = "???"
// 	}
// 	return &e.RWMutex
// }

func (m *RMap[string, V]) Set(key string, v V) bool {
	k, conflict := skiplistmap.KeyToHash(key)
	return m.Set2(k, conflict, key, v)
}

func (m *RMap[K, V]) getDirtyEntry(k, conflict uint64) (e smap.MapItem[K, V]) {

	e, ok := m.dirty.LoadItemByHash(k, conflict)

	if !ok || e == nil {
		return nil
	}
	return e

}

func (m *RMap[K, V]) getDirty(k, conflict uint64) (v V, ok bool) {

	e := m.getDirtyEntry(k, conflict)
	return e.Value(), true
}

func (m *RMap[string, V]) Set2(k, conflict uint64, kstr string, v V) bool {
	read, succ := m.read.Load().(*readMap[string, V])
	if !succ {
		m.read.Store(&readMap[string, V]{})
		read, succ = m.read.Load().(*readMap[string, V])
	}

	ensure := func(item smap.MapItem[string, V]) {
		for _, fn := range m.onNewStores {
			fn(item)
		}
	}
	if e, ok := read.store2(k, conflict, kstr, v); ok {
		defer ensure(e)
		return true
	}

	_, ok := read.m[k]
	if !ok {
		e := m.getDirtyEntry(k, conflict)
		if e != nil {
			ok = true
			e.SetValue(v)
		}
	}
	if !ok && !read.amended {
		m.storeReadFromDirty(true)
	}

	if !ok {
		e := smap.NewSampleItem(kstr, v)
		e.Init()
		m.dirty.StoreItem(e)

		if len(read.m) == 0 {
			m.storeReadFromDirty(true)
		}
		defer ensure(e)
	}
	return true
}

// func (m *RMap) ValidateDirty() {

// 	cnt := 0
// 	for cur := m.dirty.list.start.Prev().Next(); !cur.Empty(); cur = cur.Next() {
// 		next := cur.Next()
// 		if next.Empty() {
// 			break
// 		}
// 		cEntry := entryRmapFromListHead(cur)
// 		nEntry := entryRmapFromListHead(next)
// 		if cEntry.k > nEntry.k {
// 			_ = "invalid order"
// 		}
// 		cnt++
// 	}

// }

func (m *RMap[K, V]) isNotRestoreRead() bool {

	read := m.read.Load().(*readMap[K, V])
	return m.misses <= len(read.m) && m.misses < int(m.dirty.Len())
	//return m.misses < int(m.dirty.len)

}

func (m *RMap[K, V]) missLocked() {
	m.misses++
	if m.isNotRestoreRead() {
		return
	}
	m.storeReadFromDirty(false)
	m.misses = 0
}

func (m *RMap[K, V]) Get(key K) (v V, ok bool) {

	return m.Get2(smap.KeyToHash(key))
}

func (m *RMap[K, V]) Get2(k, conflict uint64) (v V, ok bool) {

	read := m.read.Load().(*readMap[K, V])
	av, ok := read.m[k]
	var e *smap.SampleItem[K, V]
	if ok {
		e, ok = av.Load().(*smap.SampleItem[K, V])
		if e.MapHead.ConflictInHamp() != conflict {
			ok = false
		} else {
			v = e.Value()
		}
	}

	if !ok && read.amended {
		// m.Lock()
		// defer m.Unlock()
		read := m.read.Load().(*readMap[K, V])
		av, ok = read.m[k]
		if !ok && read.amended {
			v, ok = m.getDirty(k, conflict)
			m.missLocked()
		}
	}
	return
}

func (m *RMap[K, V]) Delete(key string) bool {
	k, conflict := smap.KeyToHash(key)

	read, _ := m.read.Load().(*readMap[K, V])
	av, ok := read.m[k]
	if !ok && read.amended {
		m.Lock()
		defer m.Unlock()
		read, _ := m.read.Load().(*readMap[K, V])
		if !ok && read.amended {
			// av, ok = m.dirty[k]
			// delete(m.dirty, k)
			e := m.getDirtyEntry(k, conflict)
			if e != nil {
				m.dirty.AddLen(-1)
				e.Delete()
			}

			m.missLocked()
		}
	}

	if ok {
		ohead := av.Load().(*smap.SampleItem[K, V])
		//ohead.conflict = conflict
		return av.CompareAndSwap(ohead, nil)

	}
	return false
}
func (m *RMap[K, V]) Len() int {
	return int(m.dirty.Len())
}

func (m *RMap[K, V]) storeReadFromDirty(amended bool) {

	m.Lock()
	defer m.Unlock()

	for {
		oread, _ := m.read.Load().(*readMap[K, V])

		nread := &readMap[K, V]{
			m:       map[uint64]atomic.Value{},
			amended: amended,
		}

		//MENTION: not require copy oread ?
		for k, a := range oread.m {
			_, ok := a.Load().(*smap.SampleItem[K, V])
			if !ok {
				continue
			}
			nread.m[k] = a
		}
		m.dirty.RangeItem(func(item smap.MapItem[K, V]) bool {
			e, ok := item.(*smap.SampleItem[K, V])
			if ok {
				a := atomic.Value{}
				a.Store(e)
				nread.m[e.KeyInHmap()] = a

			}

			return true
		})

		if len(nread.m) == 0 {
			break
		}
		if len(oread.m) == 0 {
			nread.amended = true
		}
		if m.read.CompareAndSwap(oread, nread) {
			m.Unlock()
			m.initDirty()
			m.Lock()
			break
		}
		_ = "???"
	}
}

func (m *RMap[K, V]) initDirty() {

	m.dirty = smap.New(
		smap.OptC[K, V](smap.BucketMode(smap.CombineSearch), smap.MaxPefBucket(16)),
		smap.ItemFn(func() skiplistmap.MapItem[K, V] {
			return skiplistmap.EmptySampleHMapEntry[K, V]()
		}))
}

func (r *readMap[string, V]) store2(k, conflict uint64, kstr string, v V) (smap.MapItem[string, V], bool) {
	ov, ok := r.m[k]
	if !ok {
		return nil, ok
	}
	ohead := ov.Load().(*smap.SampleItem[string, V])
	item := smap.NewSampleItem[string, V](kstr, v)
	item.PtrListHead().Init()
	item.Setup()

	ok = ov.CompareAndSwap(ohead, item)
	return item, ok

}
