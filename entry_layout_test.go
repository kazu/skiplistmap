package skiplistmap

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
)

func TestEntryNextAcrossEmbeddedStorage(t *testing.T) {
	pool := &samepleItemPool[StringKey, int]{reusable: true}
	pool._init(3)
	before := pool.ptrItems()._at(0, false, false)
	after := pool.ptrItems()._at(1, false, false)
	for _, item := range []*embeddedEntry[StringKey, int]{before, after} {
		item.InitEntry("boundary", 0)
		item.state |= mapIsPoolItem
		item.ListHead.Init()
	}
	last := pool.ptrItems()._at(2, false, false)
	last.InitEntry("pooled", 2)
	last.state |= mapIsPoolItem
	last.ListHead.Init()
	original := NewEntry[StringKey, int]("external", 1)
	following := NewEntry[StringKey, int]("following", 3)
	var head, tail elist_head.ListHead
	t.Cleanup(func() {
		runtime.KeepAlive(pool)
		runtime.KeepAlive(original)
		runtime.KeepAlive(following)
		runtime.KeepAlive(&head)
		runtime.KeepAlive(&tail)
	})
	elist_head.InitAsEmpty(&head, &tail)
	if _, err := tail.InsertBefore(before.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.InsertBefore(original.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.InsertBefore(last.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.InsertBefore(following.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.InsertBefore(after.PtrListHead()); err != nil {
		t.Fatal(err)
	}
	if original.PtrListHead().DirectNext() != last.PtrListHead() {
		t.Fatal("fixture did not link the pooled slot after the external entry")
	}
	next := original.Next()
	if next != following || next.Value() != 3 {
		t.Fatal("public Next did not skip the internal embedded slot")
	}
	if next.Prev() != original {
		t.Fatal("Prev did not return the adjacent external entry")
	}
	if original.embeddedEntry.Next() != last || following.embeddedEntry.Prev() != last {
		t.Fatal("internal traversal skipped the embedded slot")
	}
	if SampleItemFromListHead[StringKey, int](last.PtrListHead()) != nil {
		t.Fatal("public entry conversion exposed the internal embedded slot")
	}
	if SampleItemFromListHead[StringKey, int](original.PtrListHead()) != original {
		t.Fatal("public entry conversion changed the external entry's identity")
	}
	if following.Next() != nil || original.Prev() != nil {
		t.Fatal("public traversal did not stop at the list boundary")
	}
	last.acquireRead()
	runtime.GC()
	if last.Value() != 2 || last.Copy().Value() != 2 {
		t.Fatal("internal entry traversal did not preserve the pooled value")
	}
	last.releaseRead()
	runtime.KeepAlive(pool)
	runtime.KeepAlive(original)
}

func checkEntryStorage[K Key[K], V any](t *testing.T) {
	t.Helper()
	var external Entry[K, V]
	var pooled embeddedEntry[K, V]
	if unsafe.Sizeof(external) != unsafe.Sizeof(pooled)+unsafe.Sizeof(external.retainedEntry) {
		t.Fatal("external retention changed the embedded slot's size")
	}
	if unsafe.Offsetof(external.embeddedEntry) != 0 {
		t.Fatal("shared entry data must start at the allocation's address")
	}
	t.Logf("base=%d external=%d pooled=%d", unsafe.Sizeof(external.embeddedEntry), unsafe.Sizeof(external), unsafe.Sizeof(pooled))
}

func TestEntryStorageLayouts(t *testing.T) {
	checkEntryStorage[StringKey, int](t)
	checkEntryStorage[Uint64Key, struct{}](t)
	checkEntryStorage[Uint64Key, reuseValue](t)
}

func TestEntryInitializationState(t *testing.T) {
	var e Entry[IntKey, int]
	e.state = mapIsBusy | mapIsDeleted | mapIsRetired
	var wins atomic.Int64
	var workers sync.WaitGroup
	for i := 1; i <= 16; i++ {
		workers.Go(func() {
			if e.InitEntry(IntKey(i), i) {
				wins.Add(1)
			}
		})
	}
	workers.Wait()
	if wins.Load() != 1 || e.Key() != IntKey(e.Value()) {
		t.Fatal("initialization did not publish exactly one complete key/value pair")
	}
	if e.state&(mapIsBusy|mapIsDeleted|mapIsRetired) != mapIsBusy|mapIsDeleted|mapIsRetired {
		t.Fatal("initialization discarded existing entry state")
	}
	if e.InitEntry(100, 100) {
		t.Fatal("initialized entry accepted another initialization")
	}
}
