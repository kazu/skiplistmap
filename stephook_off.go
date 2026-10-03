//go:build !stephook

package skiplistmap

import "unsafe"

const stepEnabled = false

func stepAt(point string, a, b unsafe.Pointer) {}
