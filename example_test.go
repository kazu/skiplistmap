package skiplistmap_test

import (
	"fmt"
	"github.com/kazu/skiplistmap"
	"runtime"
)

type counter struct {
	hits  int
	entry skiplistmap.Entry[skiplistmap.StringKey, int]
}

func ExampleMap_StoreItem() {
	m := skiplistmap.New[skiplistmap.StringKey, int]()
	c := &counter{hits: 1}
	c.entry.InitEntry("apple", 100)
	m.StoreItem(&c.entry)
	item, _ := m.LoadItem("apple")
	fmt.Println(item == &c.entry, c.hits, item.Value())
	m.Delete("apple")
	_, ok := m.LoadItem("apple")
	fmt.Println(ok)
	runtime.KeepAlive(c)
	// Output:
	// true 1 100
	// false
}
