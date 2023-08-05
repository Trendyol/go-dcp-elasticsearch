package helper

import (
	"io"
)

type MultiDimByteReader struct {
	s                          [][]byte
	currentSliceIndex          int
	currentIndexInCurrentSlice int
	currentSliceLen            int
	totalLen                   int
}

func (r *MultiDimByteReader) Read(b []byte) (n int, err error) {
	if r.currentSliceIndex >= r.totalLen {
		return 0, io.EOF
	}

	if r.currentIndexInCurrentSlice >= r.currentSliceLen {
		return 0, io.EOF
	}

	n = copy(b, r.s[r.currentSliceIndex][r.currentIndexInCurrentSlice:])

	if r.currentIndexInCurrentSlice+n >= r.currentSliceLen {
		r.currentSliceIndex++
		r.currentIndexInCurrentSlice = 0
		r.currentSliceLen = getLen(r.s, r.currentSliceIndex)
	} else {
		r.currentIndexInCurrentSlice += n
	}

	return
}

func getLen(b [][]byte, index int) int {
	if index >= len(b) {
		return 0
	}

	return len(b[index])
}

func (r *MultiDimByteReader) Reset(b [][]byte) {
	*r = MultiDimByteReader{b, 0, 0, getLen(b, 0), len(b)}
}

func NewMultiDimByteReader(b [][]byte) *MultiDimByteReader {
	return &MultiDimByteReader{b, 0, 0, getLen(b, 0), len(b)}
}
