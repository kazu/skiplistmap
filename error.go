package skiplistmap

import (
	"fmt"
)

type ErrType uint16

const (
	EInvalidAdd ErrType = 1 << iota
	ENotFoundBucket
	EFailBucketAlloc
	EInvalidBucket
)

var (
	ErrInvalidAdd      error = NewError(EInvalidAdd, "dd: item is added. but not found", nil)
	ErrNotFoundBUcket  error = NewError(ENotFoundBucket, "bucket is not found", nil)
	ErrFailBucketAlloc error = NewError(EFailBucketAlloc, "cannot allocated level bucket buffer", nil)
)

type Error struct {
	Type    ErrType
	message string
	error   error
}

func NewError(t ErrType, m string, e error) *Error {

	return &Error{
		Type:    t,
		message: m,
		error:   e,
	}
}

func (e *Error) Error() string {

	if e.error == nil {
		return fmt.Sprintf("type: %v message: %s", e.Type, e.message)
	}
	return fmt.Sprintf("type: %v message: %s error: %s", e.Type, e.message, e.error.Error())

}

func (e *Error) ErrorNum() uint16 {
	return uint16(e.Type)
}
