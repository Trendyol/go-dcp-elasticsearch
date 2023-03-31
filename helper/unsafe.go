package helper

import (
	"unsafe"
)

func Byte(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
