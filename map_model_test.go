package skiplistmap_test

import "github.com/kazu/skiplistmap"

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func FuzzMapOperations(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 1, 2, 1, 0, 0, 2, 0, 0, 0, 0, 3, 3, 1, 0})
	f.Add([]byte{0, 1, 1, 2, 1, 0, 0, 1, 2, 3, 1, 0, 0, 1, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 192 {
			t.Skip()
		}
		for _, p := range crashMapParams() {
			m := p.newMap().base
			model := map[string]int{}
			for i := 0; i+2 < len(data); i += 3 {
				key := fmt.Sprintf("model-%d", data[i+1]%8)
				want, exists := model[key]
				switch data[i] % 4 {
				case 0:
					value := int(data[i+2])
					if !m.Set(skiplistmap.StringKey(key), value) {
						t.Fatalf("%s operation %d: Set failed", p.name, i/3)
					}
					model[key] = value
				case 1:
					value, ok := m.Get(skiplistmap.StringKey([]byte(key)))
					if ok != exists || (ok && value != want) {
						t.Fatalf("%s operation %d: Get=(%v,%v), want (%d,%v)", p.name, i/3, value, ok, want, exists)
					}
				case 2, 3:
					remove := m.Delete
					if data[i]%4 == 3 {
						remove = m.Purge
					}
					if ok := remove(skiplistmap.StringKey([]byte(key))); ok != exists {
						t.Fatalf("%s operation %d: remove=%v, want %v", p.name, i/3, ok, exists)
					}
					delete(model, key)
				}
				seen := map[string]int{}
				m.Range(func(key skiplistmap.StringKey, value any) bool {
					k := string(key)
					if _, duplicate := seen[k]; duplicate {
						t.Errorf("%s: duplicate key %q", p.name, k)
					}
					seen[k] = value.(int)
					return true
				})
				if m.Len() != len(model) || !equalIntMaps(seen, model) {
					t.Fatalf("%s operation %d: Len=%d Range=%v, model=%v", p.name, i/3, m.Len(), seen, model)
				}
			}
		}
	})
}

func equalIntMaps(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func Test_MapsWithDifferentOptions(t *testing.T) {
	params := crashMapParams()
	runTogether(len(params), func(g int) {
		m := params[g].newMap().base
		for i := 0; i < 64; i++ {
			if !m.Set(skiplistmap.StringKey(fmt.Sprintf("shared-%d", i)), g*100+i) {
				t.Errorf("%s: Set(%d) failed", params[g].name, i)
			}
		}
		for i := 0; i < 64; i++ {
			value, ok := m.Get(skiplistmap.StringKey(fmt.Sprintf("shared-%d", i)))
			if !ok || value != g*100+i {
				t.Errorf("%s: Get(%d)=(%v,%v), want %d", params[g].name, i, value, ok, g*100+i)
			}
		}
		if n := m.Len(); n != 64 {
			t.Errorf("%s: Len=%d, want 64", params[g].name, n)
		}
	})
}

type mapHistoryOp struct {
	kind       byte
	key        string
	value      int
	result     interface{}
	ok         bool
	start, end uint64
}

func Test_MapConcurrentHistories(t *testing.T) {
	for _, p := range crashMapParams() {
		t.Run(p.name, func(t *testing.T) {
			for round := 0; round < 100; round++ {
				m := p.newMap().base
				initial := map[string]int{"a": 0}
				m.Set("a", 0)
				history := []mapHistoryOp{
					{kind: 's', key: "a", value: 1},
					{kind: 'g', key: "b"},
					{kind: 'd', key: "a"},
					{kind: 's', key: "a", value: 2},
					{kind: 's', key: "b", value: 3},
					{kind: 'g', key: "a"},
				}
				if round%2 != 0 {
					history[0].kind, history[3].kind = 'p', 'd'
					history[2].kind = 's'
				}
				var clock uint64
				runTogether(2, func(g int) {
					for i := g * 3; i < (g+1)*3; i++ {
						op := &history[i]
						op.start = atomic.AddUint64(&clock, 1)
						switch op.kind {
						case 's':
							op.ok = m.Set(skiplistmap.StringKey(op.key), op.value)
						case 'g':
							op.result, op.ok = m.Get(skiplistmap.StringKey(op.key))
						case 'd':
							op.ok = m.Delete(skiplistmap.StringKey(op.key))
						case 'p':
							op.ok = m.Purge(skiplistmap.StringKey(op.key))
						}
						op.end = atomic.AddUint64(&clock, 1)
					}
				})
				final := map[string]int{}
				m.Range(func(key skiplistmap.StringKey, value any) bool {
					final[string(key)] = value.(int)
					return true
				})
				if m.Len() != len(final) || !canLinearize(initial, final, history, 0) {
					t.Fatalf("round %d: Len=%d final=%v history=%+v", round, m.Len(), final, history)
				}
			}
		})
	}
}

func canLinearize(model, final map[string]int, history []mapHistoryOp, used uint) bool {
	if used == (1<<len(history))-1 {
		return equalIntMaps(model, final)
	}
	for i, op := range history {
		if used&(1<<i) != 0 {
			continue
		}
		ready := true
		for j, other := range history {
			if used&(1<<j) == 0 && other.end < op.start {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		value, present := model[op.key]
		if op.kind == 's' {
			if !op.ok {
				continue
			}
		} else if op.ok != present || (op.kind == 'g' && present && op.result != value) {
			continue
		}
		next := make(map[string]int, len(model)+1)
		for key, value := range model {
			next[key] = value
		}
		switch op.kind {
		case 's':
			next[op.key] = op.value
		case 'd', 'p':
			delete(next, op.key)
		}
		if canLinearize(next, final, history, used|(1<<i)) {
			return true
		}
	}
	return false
}
