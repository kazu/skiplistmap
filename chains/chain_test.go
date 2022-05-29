package chains_test

import (
	"testing"
	"unsafe"

	list_head "github.com/kazu/loncha/lista_encabezado"
	"github.com/kazu/skiplistmap/chains"
	"github.com/stretchr/testify/assert"
)

func Test_Alloc(t *testing.T) {

	list_head.MODE_CONCURRENT = true

	list := chains.New(32)

	prevs := list.Option(chains.ElementSize(int(unsafe.Sizeof(int(1)))))
	_ = prevs

	// a := (*int)(list.Alloc())
	// *a = 1

	for i := 0; i < 10; i++ {
		a := (*int)(list.Alloc())
		*a = i
	}

	for i := 0; i < 10; i++ {

		a := (*int)(list.At(i))
		assert.Equal(t, i, *a)
	}

}
