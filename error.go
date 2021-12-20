package skiplistmap

import (
	"fmt"
)

type ErrType uint16

const (
	EIItemInvalidAdd ErrType = 1 << iota
	EBucketNotFound
	EBucketAllocatedFail
	EBucketInvalid
	EBucketInvalidOrder
	EBucketAlreadyExist
	EIndexOverflow
	EIPoolAlreadyDeleted
	EIPooExpandFail
)

var (
	ErrItemInvalidAdd      error = NewError(EIItemInvalidAdd, "dd: item is added. but not found", nil)
	ErrBucketNotFound      error = NewError(EBucketNotFound, "bucket is not found", nil)
	ErrBucketAllocatedFail error = NewError(EBucketAllocatedFail, "cannot allocated level bucket buffer", nil)
	ErrBucketAlreadyExit   error = NewError(EBucketAlreadyExist, "bucket is exist alread", nil)
	ErrBucketInvalidOrder  error = NewError(EBucketInvalidOrder, "bucket is invalid order", nil)
	ErrIdxOverflow         error = NewError(EIndexOverflow, "index overflow for slice", nil)
	EPoolAlreadyDeleted    error = NewError(EIPoolAlreadyDeleted, "itemPool already deleted", nil)
	EPoolExpandFail        error = NewError(EIPooExpandFail, "fail expand itemPool", nil)
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
