package rmap_test

import (
	"fmt"
	"testing"

	"github.com/kazu/skiplistmap/rmap"
	"github.com/stretchr/testify/assert"
)

func Test_Get_Set(t *testing.T) {

	rmap := rmap.New[string, int]()

	for i := 0; i < 1000; i++ {
		rmap.Set(fmt.Sprintf("hoge%d", i), i)
	}

	for i := 0; i < 1000; i++ {
		_, ok := rmap.Get(fmt.Sprintf("hoge%d", i))
		assert.True(t, ok)
	}

}
