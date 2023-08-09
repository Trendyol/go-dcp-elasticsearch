package helper

import (
	"unsafe"
)

func Byte(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func String(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
