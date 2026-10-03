# Skip List Map in Golang

Skip List Map はキーと値を型で指定する並行Mapです。登録・取得・更新・削除を複数のgoroutineから実行できます。値が参照するsliceやpointerの参照先の同期は利用者が行います。


## status
[![Go](https://github.com/kazu/skiplistmap/actions/workflows/go.yml//badge.svg?branch=master)](https://github.com/kazu/skiplistmap/actions/workflows/go.yml/)
[![Go Reference](https://pkg.go.dev/badge/github.com/kazu/skiplistmap.svg)](https://pkg.go.dev/badge/github.com/kazu/skiplistmap)

## features

- buckets, elemenet(key/value item) structure is concurrent embeded-linked list. (using [list_encabezado])
- keep key order by hash function.
- ability to store value ( value of key/vale) and elemet of ket/value item(detail is later)
- improve performance for sync.Map/ internal map in write heavy environment.

## requirement

Go 1.27.1 for this development branch. Its local dependency checkouts and
verification commands are described in [stability checks](docs/stability-checks.md).

## install 

この型付きAPIは開発版です。公開版を取得する `go get` では、このブランチのAPIは
入りません。[開発環境と検証手順](docs/stability-checks.md)に従って
依存checkoutを用意してください。次のバージョンは `VERSION` の0.8.0です。
[配布前の確認事項](docs/releasing.md)も参照してください。


## basic usage


```go
package main

import (
    "fmt"
    "runtime"

    "github.com/kazu/skiplistmap"
)

func main() {
    sMap := skiplistmap.New[skiplistmap.StringKey, int](skiplistmap.MaxPefBucket[skiplistmap.StringKey, int](12))
    sMap.Set("test1", 1)
    sMap.Set("test2", 2)
    if value, ok := sMap.Get("test1"); ok {
        fmt.Println(value)
    }

    // 外部Entryは利用者が保持する。ItemFnは不要。
    sMap2 := skiplistmap.New[skiplistmap.StringKey, int]()
    item := skiplistmap.NewEntry[skiplistmap.StringKey, int]("test1", 1234)
    sMap2.StoreItem(item)
    if loaded, ok := sMap2.LoadItem("test1"); ok {
        fmt.Println(loaded.Key(), loaded.Value())
    }

    sMap2.RangeItem(func(item *skiplistmap.Entry[skiplistmap.StringKey, int]) bool {
        fmt.Printf("key=%v\n", item.Key())
        return true
    })
    sMap2.Range(func(key skiplistmap.StringKey, value int) bool {
        fmt.Printf("key=%v value=%v\n", key, value)
        return true
    })

    sMap.Delete("test2")
    _, found := sMap.Get("test2")
    fmt.Println(found)
    sMap2.Purge("test1")
    runtime.KeepAlive(item)
}
```

## rmap

readとdirtyの世代を分けるrmapも、キーと値を型で指定できます。
キーの条件はMapと同じで、不在時は値の型のゼロ値とfalseを返します。

```go
package main

import (
    "fmt"
    "github.com/kazu/skiplistmap"
    "github.com/kazu/skiplistmap/rmap"
)

func main() {
    m := rmap.New[skiplistmap.StringKey, int]()
    m.Set("apple", 1)
    fmt.Println(m.Get("apple"))
    m.Delete("apple")
    fmt.Println(m.Get("apple"))
}
```

## performance

以下のグラフは従来実装の測定記録です。型付き本体の性能を示すものではありません。
型付きAPI、コピー公開、Entryの保持と再利用については[型付きAPI](docs/typed-api.md)を参照してください。

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
