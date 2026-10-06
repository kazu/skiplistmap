package skiplistmap

import "testing"

// findIdx finds the first slot whose reverse is not below the one asked for,
// holes included, and splitAnchor the last slot before it that is still on
// the list.
func TestPoolSplitIndexAndAnchor(t *testing.T) {
	p, _ := makeInsertPool(8, 8)
	// the reverses are 2, 4, ..., 16; slot 3 is a tombstone of Delete, still
	// on the list, slot 4 a hole of Purge, off the list
	p.ptrItems().at(3).Delete()
	p.ptrItems().at(4).Delete()
	p.ptrItems().at(4).ListHead.MarkForDelete()
	p.ptrItems().at(4).ListHead.Init()

	for _, tc := range []struct {
		reverse uint64
		idx     int
	}{
		{1, 0}, {2, 0}, {3, 1}, {9, 4}, {10, 4}, {11, 5}, {16, 7},
	} {
		idx, err := p.findIdx(tc.reverse)
		if err != nil || idx != tc.idx {
			t.Fatalf("findIdx(%d) = %d, %v; want %d", tc.reverse, idx, err, tc.idx)
		}
	}
	if idx, err := p.findIdx(17); err != ErrIdxOverflow || idx != -1 {
		t.Fatalf("findIdx(17) = %d, %v; want -1, ErrIdxOverflow", idx, err)
	}

	if a := p.splitAnchor(0); a != nil {
		t.Fatal("splitAnchor(0) found a slot before the first")
	}
	if a := p.splitAnchor(3); a != &p.ptrItems().at(2).ListHead {
		t.Fatal("splitAnchor(3) is not slot 2")
	}
	// the tombstone and the hole are passed over
	if a := p.splitAnchor(5); a != &p.ptrItems().at(2).ListHead {
		t.Fatal("splitAnchor(5) is not slot 2")
	}
	if a := p.splitAnchor(7); a != &p.ptrItems().at(6).ListHead {
		t.Fatal("splitAnchor(7) is not slot 6")
	}
}
