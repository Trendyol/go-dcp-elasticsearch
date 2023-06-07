package bulk

import (
	"unsafe"
)

func StringToByte(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func ByteToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
