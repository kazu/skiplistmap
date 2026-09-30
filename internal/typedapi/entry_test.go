package typedapi

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/kazu/elist_head"
)

type user struct {
	Name string
	Age  int
}

func TestValueAndPointerStorage(t *testing.T) {
	u := user{Name: "alice", Age: 20}
	inline := NewEntry(StringKey("a"), u)
	external := NewEntry(StringKey("a"), &u)
	u.Age++
	if inline.Value().Age != 20 || external.Value() != &u || external.Value().Age != 21 {
		t.Fatal("inline values and external pointers must retain their chosen semantics")
	}
	if unsafe.Sizeof(external.value) != unsafe.Sizeof(&u) {
		t.Fatal("external value slot must contain only the pointer")
	}
	if unsafe.Sizeof(inline.value) != unsafe.Sizeof(u) {
		t.Fatal("inline value slot must contain the User itself")
	}
}

func checkEntryLinks[K Key[K], V any](t *testing.T, key K, value V) {
	t.Helper()
	entries := make([]Entry[K, V], 3)
	view := NewEntryView[K, V]()
	head, item, tail := &entries[0], &entries[1], &entries[2]
	elist_head.InitAsEmpty(&head.ListHead, &tail.ListHead)
	item.key, item.value = key, value
	item.ListHead.Init()
	if err := view.InsertBefore(tail, item); err != nil {
		t.Fatal(err)
	}
	if view.Element(&item.ListHead) != item || view.DirectNext(head) != item || view.DirectPrev(tail) != item {
		t.Fatal("typed recovery must retain the exact entry identity")
	}
	if got := view.RecoverField(&item.value, 0); got != item {
		t.Fatal("inline value recovery")
	}
	runtime.GC()
	if view.Element(&item.ListHead) != item || !item.Key().Equal(key) {
		t.Fatal("typed link recovery after GC")
	}
	runtime.KeepAlive(entries)
}

func TestEntryLayouts(t *testing.T) {
	checkEntryLinks(t, StringKey("a"), byte(1))
	checkEntryLinks(t, Uint64Key(7), user{Name: "alice", Age: 20})
	checkEntryLinks(t, BytesKey("a"), struct {
		Tag  byte
		Data [129]uint64
	}{Tag: 1})
	u := user{Name: "outside"}
	checkEntryLinks(t, StringKey("a"), &u)
}

func TestRecoverInlineField(t *testing.T) {
	e := NewEntry(StringKey("a"), user{Name: "alice", Age: 20})
	view := NewEntryView[StringKey, user]()
	name := &e.value.Name
	age := &e.value.Age
	if view.RecoverField(name, unsafe.Offsetof(user{}.Name)) != e ||
		view.RecoverField(age, unsafe.Offsetof(user{}.Age)) != e {
		t.Fatal("generic field recovery must point to the containing Entry")
	}
	runtime.KeepAlive(e)
}
