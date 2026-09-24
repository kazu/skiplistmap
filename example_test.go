package skiplistmap_test

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/kazu/elist_head"
	"github.com/kazu/skiplistmap"
)

// counter is a user-defined struct. SampleItem must be its first field, so
// that a pointer to the SampleItem is a pointer to the counter.
type counter struct {
	skiplistmap.SampleItem
	hits int
}

// HmapEntryFromListHead returns the counter that embeds lhead, so that
// LoadItem returns *counter.
func (c *counter) HmapEntryFromListHead(lhead *elist_head.ListHead) skiplistmap.HMapEntry {
	return (*counter)(unsafe.Pointer(skiplistmap.SampleItemFromListHead(lhead)))
}

func ExampleMap_StoreItem() {
	m := skiplistmap.New(skiplistmap.ItemFn(func() skiplistmap.MapItem { return (*counter)(nil) }))

	c := &counter{hits: 1}
	c.K = "apple"
	c.SetValue(100)
	m.StoreItem(c)

	item, _ := m.LoadItem("apple")
	fmt.Println(item.(*counter) == c, item.(*counter).hits, item.Value())

	m.Delete("apple")
	_, ok := m.LoadItem("apple")
	fmt.Println(ok)

	runtime.KeepAlive(c)
	// Output:
	// true 1 100
	// false
}
