## Skip List Map in Golang

Skip List Map is an ordered and concurrent map.  this Map is gourtine safety for reading/updating/deleting, require locking and coordination. This

## features

- buckets, elemenete(key/value item) structure is concurrent embeded-linked list.
- keep key order by hash function.
- ability to store value ( value of key/vale) and elemet of ket/value item(detail is later)
- improve performance for sync.Map/ internal map in write heavy environment.

## require mennt

` golang >= 1.17`

## install 

Install this package through go get.

```
go get "github.com/kazu/skiplistmap"

```


## basic usage


```
package main 

import (
    "fmt"
)

//create skip list map
sMap := skiplistmap.New()
// create make with configure MaxPerBucket
// sMap := skiplistmap.New(skiplistmap.MaxPefBucket(12))
// sMap := skiplistmap.New(skiplistmap.MaxPefBucket(12))

// Set/Add values
sMap.Set("test1", 1)
sMap.Set("test2", 2)

// get the value for a key, return nil if not found, the ok is found.
inf, ok := sMap.Get("test1)
var value1 int
if ok {
    value1 = inf.(int)
}

ok = sMap.GetByFn(func(v interface{}) {
    value = v.(int)
})


// if directry using key/value item. use SampleItem struct
sMap2 := skiplistmap.New(skiplistmap.MaxPefBucket(12))
item := &skiplistmap.SampleItem{
    K: "test1", 
    V: 1234
}

// store item
ok = sMap2.StoreItem(item)

// get key/value item
item, ok = sMap.LoadItem("test1")
// get next key/value
nItem := sMap.Next()


// delete marking. set nil as value.
sMap.Delete("test2")

// delete key/value entry from map. traverse locked for deleting item to acceess concurrent
sMap.Purge("test2")


```