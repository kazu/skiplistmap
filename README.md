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
- skiplistmap normal element search
- skiplistmap3 with reverse element search
- RMap  rewrite drity of sync.Map as skiplistmap (sync.Map read is map[uint64]atomic.Value)

### read only
```
Benchmark_Map/mapWithMutex__________________w/_0_bucket=__0-16         	17055374	        69.39 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/sync.Map______________________w/_0_bucket=__0-16         	25268019	        42.00 ns/op	      63 B/op	       2 allocs/op
Benchmark_Map/skiplistmap___________________w/_0_bucket=_32-16         	26189863	        47.96 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap___________________w/_0_bucket=_16-16         	32570624	        44.40 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/skiplistmap3__________________w/_0_bucket=_16-16         	36449119	        40.41 ns/op	      15 B/op	       1 allocs/op
Benchmark_Map/RMap__________________________w/_0_bucket=__0-16         	34806978	        33.39 ns/op	      31 B/op	       2 allocs/op
```


### read 50%. update 50%

```
Benchmark_Map/mapWithMutex__________________w/50_bucket=__0-16         	 3314656	       364.6 ns/op	      16 B/op	       1 allocs/op
Benchmark_Map/sync.Map______________________w/50_bucket=__0-16         	14289441	        70.34 ns/op	     134 B/op	       4 allocs/op
Benchmark_Map/skiplistmap___________________w/50_bucket=_32-16         	21370478	        64.18 ns/op	      27 B/op	       2 allocs/op
Benchmark_Map/skiplistmap___________________w/50_bucket=_16-16         	21110790	        65.22 ns/op	      27 B/op	       2 allocs/op
Benchmark_Map/RMap__________________________w/50_bucket=__0-16         	21455697	        53.36 ns/op	      72 B/op	       4 allocs/op
```


[list_encabezado]: github.com/kazu/loncha/lista_encabezado