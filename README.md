# Skip List Map in Golang

Skip List Map is a concurrent map.  this Map is goroutine safety for reading/updating/deleting, no-require locking and coordination.


## status
[![Go](https://github.com/kazu/skiplistmap/actions/workflows/go.yml//badge.svg?branch=master)](https://github.com/kazu/skiplistmap/actions/workflows/go.yml/)
[![Go Reference](https://pkg.go.dev/badge/github.com/kazu/skiplistmap.svg)](https://pkg.go.dev/badge/github.com/kazu/skiplistmap)

## features

- buckets, elemenet(key/value item) structure is concurrent embeded-linked list. (using [list_encabezado])
- keep key order by hash function.
- ability to store value ( value of key/vale) and elemet of ket/value item(detail is later)
- improve performance for sync.Map/ internal map in write heavy environment.

## requirement

` golang >= 1.17`

## install 

Install this package through go get.

```
go get "github.com/kazu/skiplistmap"

```


## basic usage


```go
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
inf, ok := sMap.Get("test1")
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

// traverse all item or key/value 
sMap.RangeItem(func(item MapItem) bool {
  fmt.Printf("key=%+v\n", item.Key())  
})
sMap.Range(func(key, value interface{}) bool {
  fmt.Printf("key=%+v\n", key)  
})



// delete marking. set nil as value.
sMap.Delete("test2")

// delete key/value entry from map. traverse locked for deleting item to acceess concurrent
sMap.Purge("test2")


```

## performance

### condition
- 100000 record. set key/value before benchmark
- mapWithMutex map[interface{}]interface{} with sync.RWMutex
- skiplistmap4 this package's item pool mode
- skiplistmap5 embedded item pool in bucket.
- hashmap [github.com/cornelk/hashmap] package
- cmap [github.com/lrita/cmap] package 

### read only
```
Benchmark_Map/mapWithMutex_w/_0_bucket=__0-16         	15610328	        76.12 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/sync.Map_____w/_0_bucket=__0-16         	25813341	        43.37 ns/op	      63 B/op	       2 allocs/op
Benchmark_Map/skiplistmap4_w/_0_bucket=_16-16         	35947046	        38.25 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap4_w/_0_bucket=_32-16         	36800390	        36.61 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap5_w/_0_bucket=_16-16         	46779364	        27.37 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap5_w/_0_bucket=_32-16         	49452940	        27.01 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap5_w/_0_bucket=_64-16         	47740882	        27.45 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/hashmap______w/_0_bucket=__0-16         	20071834	        63.11 ns/op	      31 B/op	       2 allocs/op
Benchmark_Map/cmap.Cmap____w/_0_bucket=__0-16            1841415	       721.00 ns/op	     935 B/op	       5 allocs/op

```


### read 50%. update 50%

```
Benchmark_Map/mapWithMutex_w/50_bucket=__0-16         	 2895382	       377.3  ns/op	      16 B/op	       1 allocs/op
Benchmark_Map/sync.Map_____w/50_bucket=__0-16         	 9532836	       137.4  ns/op	     140 B/op	       4 allocs/op
Benchmark_Map/skiplistmap4_w/50_bucket=_16-16         	33024600	        50.80 ns/op	      21 B/op	       2 allocs/op
Benchmark_Map/skiplistmap4_w/50_bucket=_32-16         	33231843	        48.75 ns/op	      21 B/op	       2 allocs/op
Benchmark_Map/skiplistmap5_w/50_bucket=_16-16         	33412243	        38.20 ns/op	      21 B/op	       2 allocs/op
Benchmark_Map/skiplistmap5_w/50_bucket=_32-16         	34377592	        38.60 ns/op	      20 B/op	       2 allocs/op
Benchmark_Map/skiplistmap5_w/50_bucket=_64-16         	32261986	        39.33 ns/op	      20 B/op	       2 allocs/op
Benchmark_Map/hashmap______w/50_bucket=__0-16         	37279302	        66.94 ns/op	      65 B/op	       3 allocs/op
Benchmark_Map/cmap_________w/50_bucket=__0-16            1592382	       733.2  ns/op	    1069 B/op	       7 allocs/op
```

## why faster ?


- lock free , thread safe concurrent without lock. embedded pool mode is using lock per bucket
- buckets, items(key/value items) is doubly linked-list. this linked list is embedded type. so faster
- items is shards per hash key single bytes. items in the same shard is high-locality because in same slice.
- next/prev pointer of items's linked list is relative pointer. low-cost copy for expand shad slice. ([elist_head])

[list_encabezado]: https://pkg.go.dev/github.com/kazu/loncha@v0.4.5/lista_encabezado
[elist_head]: https://github.com/kazu/elist_head
[github.com/cornelk/hashmap]: https://github.com/cornelk/hashmap
[github.com/lrita/cmap]: https://github.com/lrita/cmap