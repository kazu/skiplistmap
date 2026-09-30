package typedapi

import (
	"bytes"
	"testing"

	"github.com/kazu/skiplistmap"
)

func checkHash[K Key[K], R any](t *testing.T, key K, raw R) {
	t.Helper()
	hash, conflict := key.KeyHash()
	wantHash, wantConflict := skiplistmap.KeyToHash(raw)
	if hash != wantHash || conflict != wantConflict || !key.Equal(key) {
		t.Fatalf("key=%v hash=(%x,%x), want=(%x,%x)", key, hash, conflict, wantHash, wantConflict)
	}
}

func TestStandardKeysPreserveHashes(t *testing.T) {
	for _, value := range []string{"", "apple", "日本語", "a\x00b"} {
		checkHash(t, StringKey(value), value)
		checkHash(t, BytesKey([]byte(value)), []byte(value))
	}
	checkHash(t, BytesKey(nil), []byte(nil))
	checkHash(t, Uint64Key(1<<64-1), uint64(1<<64-1))
	checkHash(t, ByteKey(255), byte(255))
	checkHash(t, IntKey(-1), int(-1))
	checkHash(t, Int32Key(-2147483648), int32(-2147483648))
	checkHash(t, Uint32Key(1<<32-1), uint32(1<<32-1))
	checkHash(t, Int64Key(-1<<63), int64(-1<<63))
}

func TestByteKeysCompareContents(t *testing.T) {
	aData, bData := [5]byte{'a', 'p', 'p', 'l', 'e'}, [5]byte{'a', 'p', 'p', 'l', 'e'}
	a, b := BytesKey(aData[:]), BytesKey(bData[:])
	if &a[0] == &b[0] || !a.Equal(b) || a.Equal(BytesKey("other")) {
		t.Fatal("byte keys must compare contents, not backing-array addresses")
	}
	if !BytesKey(nil).Equal(BytesKey{}) {
		t.Fatal("nil and empty byte keys must remain equal")
	}
	h1, c1 := BytesKey(nil).KeyHash()
	h2, c2 := (BytesKey{}).KeyHash()
	if h1 != h2 || c1 != c2 {
		t.Fatal("equal byte keys must have equal hash pairs")
	}
}

type collisionKey struct{ Data []byte }

func (k collisionKey) KeyHash() (uint64, uint64) { return 7, 11 }
func (k collisionKey) Equal(other collisionKey) bool {
	return bytes.Equal(k.Data, other.Data)
}

func TestCustomKeysCompareAfterHashCollision(t *testing.T) {
	a := NewEntry(collisionKey{[]byte("alice")}, 1)
	b := NewEntry(collisionKey{[]byte("bob")}, 2)
	ah, ac := a.Key().KeyHash()
	bh, bc := b.Key().KeyHash()
	if ah != bh || ac != bc || a.Key().Equal(b.Key()) {
		t.Fatal("same hash pair must not make distinct keys equal")
	}
	if !a.Key().Equal(collisionKey{[]byte("alice")}) {
		t.Fatal("custom key lost content equality")
	}
}
