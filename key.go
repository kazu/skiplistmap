package skiplistmap

import (
	"bytes"

	"github.com/cespare/xxhash"
)

// Key requires a hash pair and equality on the same concrete key type.
// Equal keys must produce the same hash pair.
type Key[K any] interface {
	KeyHash() (uint64, uint64)
	Equal(K) bool
}

type StringKey string

func (k StringKey) KeyHash() (uint64, uint64) {
	return MemHashString(string(k)), xxhash.Sum64String(string(k))
}

func (k StringKey) Equal(other StringKey) bool { return k == other }

// BytesKey borrows its bytes. They must not change while the key is stored.
type BytesKey []byte

func (k BytesKey) KeyHash() (uint64, uint64) {
	return MemHash(k), xxhash.Sum64(k)
}

func (k BytesKey) Equal(other BytesKey) bool { return bytes.Equal(k, other) }

type integer interface {
	~uint64 | ~uint8 | ~int | ~int32 | ~uint32 | ~int64
}

func hashInteger[N integer](number N) (uint64, uint64) { return uint64(number), 0 }

type Uint64Key uint64
type ByteKey byte
type IntKey int
type Int32Key int32
type Uint32Key uint32
type Int64Key int64

func (k Uint64Key) KeyHash() (uint64, uint64) { return hashInteger(k) }
func (k ByteKey) KeyHash() (uint64, uint64)   { return hashInteger(k) }
func (k IntKey) KeyHash() (uint64, uint64)    { return hashInteger(k) }
func (k Int32Key) KeyHash() (uint64, uint64)  { return hashInteger(k) }
func (k Uint32Key) KeyHash() (uint64, uint64) { return hashInteger(k) }
func (k Int64Key) KeyHash() (uint64, uint64)  { return hashInteger(k) }

func (k Uint64Key) Equal(other Uint64Key) bool { return k == other }
func (k ByteKey) Equal(other ByteKey) bool     { return k == other }
func (k IntKey) Equal(other IntKey) bool       { return k == other }
func (k Int32Key) Equal(other Int32Key) bool   { return k == other }
func (k Uint32Key) Equal(other Uint32Key) bool { return k == other }
func (k Int64Key) Equal(other Int64Key) bool   { return k == other }
