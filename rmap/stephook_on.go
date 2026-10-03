//go:build stephook

package rmap

var stepHook func(string)

func stepAt(point string) {
	if stepHook != nil {
		stepHook(point)
	}
}
