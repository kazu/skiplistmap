// Package typedapi verifies the storage and constraints for the planned typed
// map API. It is a design probe, not a second map implementation.
package typedapi

import "github.com/kazu/skiplistmap"

type Key[K any] = skiplistmap.Key[K]
type StringKey = skiplistmap.StringKey
type BytesKey = skiplistmap.BytesKey
type Uint64Key = skiplistmap.Uint64Key
type ByteKey = skiplistmap.ByteKey
type IntKey = skiplistmap.IntKey
type Int32Key = skiplistmap.Int32Key
type Uint32Key = skiplistmap.Uint32Key
type Int64Key = skiplistmap.Int64Key
