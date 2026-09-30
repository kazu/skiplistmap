package badkeys

import "github.com/kazu/skiplistmap/internal/typedapi"

type missingHash struct{}

func (missingHash) Equal(missingHash) bool { return true }

type wrongEqual struct{}

func (wrongEqual) KeyHash() (uint64, uint64) { return 0, 0 }
func (wrongEqual) Equal(any) bool            { return true }

var (
	_ typedapi.Entry[string, int]
	_ typedapi.Entry[missingHash, int]
	_ typedapi.Entry[wrongEqual, int]
)
