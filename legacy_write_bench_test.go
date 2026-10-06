package skiplistmap_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cornelk/hashmap"
	list_head "github.com/kazu/lista_encabezado"
	"github.com/kazu/skiplistmap"
)

// Fresh maps keep calibration passes from turning new-key inserts into updates.
func freshLegacyWriteMap(p legacyMapTestParam) list_head.MapGetSet {
	switch p.mapInf.(type) {
	case *list_head.MapWithLock:
		return &list_head.MapWithLock{}
	case legacySyncMap:
		return legacySyncMap{}
	case *syncMap:
		return &syncMap{}
	case legacyHashMap:
		return legacyHashMap{m: &hashmap.HashMap{}}
	case legacyCMap:
		return legacyCMap{}
	case *referenceCMap:
		return &referenceCMap{}
	case *legacyTypedMap:
		return &legacyTypedMap{base: skiplistmap.NewHMap[skiplistmap.StringKey, *list_head.ListHead](
			skiplistmap.BucketMode[skiplistmap.StringKey, *list_head.ListHead](p.mode),
			skiplistmap.MaxPefBucket[skiplistmap.StringKey, *list_head.ListHead](p.buckets),
			skiplistmap.MinCapItems[skiplistmap.StringKey, *list_head.ListHead](p.buckets),
			skiplistmap.UseEmbeddedPool[skiplistmap.StringKey, *list_head.ListHead](p.mode == skiplistmap.CombineSearch3),
		)}
	default:
		panic("unsupported legacy write benchmark adapter")
	}
}

func benchmarkLegacyWrites(b *testing.B, benchmarks []legacyMapTestParam, workers, records int) {
	if records != 100000 {
		return
	}
	for _, original := range benchmarks {
		if original.percent != 0 {
			continue
		}
		for _, update := range []bool{false, true} {
			p := original
			p.concurrent, p.cnt, p.percent, p.isUpdate = workers, records, 100, update
			kind := "new"
			if update {
				kind = "update"
			}
			b.Run("write100/"+kind+"/"+p.String(), func(b *testing.B) {
				p.mapInf = freshLegacyWriteMap(p)
				if update {
					runBnech(b, &p)
				} else {
					runLegacyInsertOnly(b, &p)
				}
			})
		}
	}
}

// Start empty and give each worker a disjoint, nonwrapping sequence of keys.
// Each calibration pass gets a fresh map and attempts b.N new inserts.
func runLegacyInsertOnly(b *testing.B, p *legacyMapTestParam) {
	m := p.mapInf
	parallelism := (p.concurrent + runtime.GOMAXPROCS(0) - 1) / runtime.GOMAXPROCS(0)
	actualWorkers := parallelism * runtime.GOMAXPROCS(0)
	b.SetParallelism(parallelism)
	var nextWorker atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := nextWorker.Add(1) - 1
		for pb.Next() {
			m.Set(fmt.Sprintf("%d", index), &list_head.ListHead{})
			index += uint64(actualWorkers)
		}
	})
	b.StopTimer()
}
