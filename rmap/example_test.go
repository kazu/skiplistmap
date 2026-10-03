package rmap_test

import (
	"fmt"
	smap "github.com/kazu/skiplistmap"

	"github.com/kazu/skiplistmap/rmap"
)

func ExampleRMap() {
	m := rmap.New[smap.StringKey, int]()
	m.Set("apple", 1)
	fmt.Println(m.Get("apple"))
	m.Set("apple", 2)
	m.Set("orange", 3)
	fmt.Println(m.Get("orange"))
	fmt.Println(m.Len())
	m.Delete("apple")
	fmt.Println(m.Get("apple"))
	fmt.Println(m.Len())
	// Output:
	// 1 true
	// 3 true
	// 2
	// 0 false
	// 1
}
