package skiplistmap_test

import (
	"fmt"
	"testing"

	"github.com/kazu/skiplistmap"
)

// BenchmarkPoolSlideStats counts middle-insert allocation decisions. Its
// timings include instrumentation and are not the ordinary insert benchmark.
func BenchmarkPoolSlideStats(b *testing.B) {
	previous := skiplistmap.EnableStats
	defer func() { skiplistmap.EnableStats = previous }()
	for _, size := range []int{32, 128} {
		b.Run(fmt.Sprintf("bucket=%d", size), func(b *testing.B) {
			// Build the map before enabling stats to avoid the initial dump.
			skiplistmap.EnableStats = false
			p := legacyMapTestParam{concurrent: 16, buckets: size, mode: skiplistmap.CombineSearch3, mapInf: &legacyTypedMap{}}
			p.mapInf = freshLegacyWriteMap(p)
			skiplistmap.ResetStats()
			skiplistmap.EnableStats = true
			runLegacyInsertOnly(b, &p)
			b.StopTimer()
			slides := skiplistmap.DebugStats[skiplistmap.CntPoolSlide].Load()
			allocs := skiplistmap.DebugStats[skiplistmap.CntPoolInsertAlloc].Load()
			b.ReportMetric(float64(slides), "slides")
			b.ReportMetric(float64(allocs), "insert-allocs")
			if total := slides + allocs; total != 0 {
				b.ReportMetric(100*float64(slides)/float64(total), "slide-%")
			}
		})
	}
}
