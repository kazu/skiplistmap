package rmap

import (
	"sync"
	"sync/atomic"

	"github.com/kazu/skiplistmap"
	smap "github.com/kazu/skiplistmap"
)

type baseMap struct {
	sync.RWMutex
	read        atomic.Value
	onNewStores []func(smap.MapItem)

	misses int
}

type RMap struct {
	baseMap
	limit int
	dirty *smap.Map
}

type readMap struct {
	m       map[uint64]atomic.Value
	amended bool // include dirty map
}

func New() (rmap *RMap) {

	rmap = &RMap{
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

func (m *RMap) Set(key string, v interface{}) bool {
	k, conflict := skiplistmap.KeyToHash(key)
	return m.Set2(k, conflict, key, v)
}

func (m *RMap) getDirtyEntry(k, conflict uint64) (e smap.MapItem) {

	e, ok := m.dirty.LoadItemByHash(k, conflict)

	if !ok || e == nil {
		return nil
	}
	return e

}

func (m *RMap) getDirty(k, conflict uint64) (v interface{}, ok bool) {

	e := m.getDirtyEntry(k, conflict)
	return e.Value(), true
}

func (m *RMap) Set2(k, conflict uint64, kstr string, v interface{}) bool {
	read, succ := m.read.Load().(*readMap)
	if !succ {
		m.read.Store(&readMap{})
		read, succ = m.read.Load().(*readMap)
	}

	ensure := func(item smap.MapItem) {
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
		e := &smap.SampleItem{
			K: kstr,
			V: v,
		}
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

func (m *RMap) isNotRestoreRead() bool {

	read := m.read.Load().(*readMap)
	return m.misses <= len(read.m) && m.misses < int(m.dirty.Len())
	//return m.misses < int(m.dirty.len)

}

func (m *RMap) missLocked() {
	m.misses++
	if m.isNotRestoreRead() {
		return
	}
	m.storeReadFromDirty(false)
	m.misses = 0
}

func (m *RMap) Get(key string) (v interface{}, ok bool) {

	return m.Get2(smap.KeyToHash(key))
}

func (m *RMap) Get2(k, conflict uint64) (v interface{}, ok bool) {

	read := m.read.Load().(*readMap)
	av, ok := read.m[k]
	var e *smap.SampleItem
	if ok {
		e, ok = av.Load().(*smap.SampleItem)
		if e.MapHead.ConflictInHamp() != conflict {
			ok = false
		} else {
			v = e.V
		}
	}

	if !ok && read.amended {
		// m.Lock()
		// defer m.Unlock()
		read := m.read.Load().(*readMap)
		av, ok = read.m[k]
		if !ok && read.amended {
			v, ok = m.getDirty(k, conflict)
			m.missLocked()
		}
	}
	return
}

func (m *RMap) Delete(key string) bool {
	k, conflict := smap.KeyToHash(key)

	read, _ := m.read.Load().(*readMap)
	av, ok := read.m[k]
	if !ok && read.amended {
		m.Lock()
		defer m.Unlock()
		read, _ := m.read.Load().(*readMap)
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
		ohead := av.Load().(*smap.SampleItem)
		//ohead.conflict = conflict
		return av.CompareAndSwap(ohead, nil)

	}
	return false
}
func (m *RMap) Len() int {
	return int(m.dirty.Len())
}

func (m *RMap) storeReadFromDirty(amended bool) {

	m.Lock()
	defer m.Unlock()

	for {
		oread, _ := m.read.Load().(*readMap)

		nread := &readMap{
			m:       map[uint64]atomic.Value{},
			amended: amended,
		}

		//MENTION: not require copy oread ?
		for k, a := range oread.m {
			_, ok := a.Load().(*smap.SampleItem)
			if !ok {
				continue
			}
			nread.m[k] = a
		}
		m.dirty.RangeItem(func(item smap.MapItem) bool {
			e, ok := item.(*smap.SampleItem)
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

func (m *RMap) initDirty() {

	m.dirty = smap.New(
		smap.BucketMode(smap.CombineSearch),
		smap.MaxPefBucket(16),
		smap.ItemFn(func() skiplistmap.MapItem {
			return skiplistmap.EmptySampleHMapEntry
		}))
}

func (r *readMap) store2(k, conflict uint64, kstr string, v interface{}) (smap.MapItem, bool) {
	ov, ok := r.m[k]
	if !ok {
		return nil, ok
	}
	ohead := ov.Load().(*smap.SampleItem)
	item := &smap.SampleItem{K: kstr, V: v}
	item.PtrListHead().Init()
	item.Setup()

	ok = ov.CompareAndSwap(ohead, item)
	return item, ok

}
