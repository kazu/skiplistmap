package atomic_util

import (
	"sync/atomic"
	"unsafe"
)

const intSize = unsafe.Sizeof(int(0))

// LoadInt ... load int variable with atomic
func LoadInt(i *int) int {
	if intSize == unsafe.Sizeof(int32(0)) {
		return int(atomic.LoadInt32((*int32)(unsafe.Pointer(i))))
	} else if intSize == unsafe.Sizeof(int64(0)) {
		return int(atomic.LoadInt64((*int64)(unsafe.Pointer(i))))
	}
	panic("unknow int size . only 32/64 bit")
}

func StoreInt(i *int, new int) {
	if intSize == unsafe.Sizeof(int32(0)) {
		atomic.StoreInt32((*int32)(unsafe.Pointer(i)), int32(new))
	} else if intSize == unsafe.Sizeof(int64(0)) {
		atomic.StoreInt64((*int64)(unsafe.Pointer(i)), int64(new))
	}

}

// CompareAndSwapInt ... atomic.CompareAndSwap for int
func CompareAndSwapInt(i *int, old int, new int) bool {
	if intSize == unsafe.Sizeof(int32(0)) {
		//return int(atomic.LoadInt32((*int32)(unsafe.Pointer(i))))
		return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(i)), int32(old), int32(new))
	} else if intSize == unsafe.Sizeof(int64(0)) {
		return atomic.CompareAndSwapInt64((*int64)(unsafe.Pointer(i)), int64(old), int64(new))
	}
	panic("unknow int size . only 32/64 bit")
}
