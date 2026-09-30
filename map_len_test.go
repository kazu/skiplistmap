package skiplistmap_test

import "testing"

func BenchmarkMapLen(b *testing.B) {
	m := newPoolMap(32)
	m.base.Set("key", 1)
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		total += m.base.Len()
	}
	if total != b.N {
		b.Fatalf("sum of Len=%d, want %d", total, b.N)
	}
}

func Test_LenDuringSet(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			m := p.newMap()
			m.base.Set("before", 1)
			runTogether(2, func(g int) {
				if g == 0 {
					m.base.Set("after", 2)
				} else {
					if n := m.base.Len(); n < 1 || n > 2 {
						t.Errorf("Len=%d during insertion, want 1 or 2", n)
					}
				}
			})
			if n := m.base.Len(); n != 2 {
				t.Fatalf("Len=%d after insertion, want 2", n)
			}
		})
	}
}
