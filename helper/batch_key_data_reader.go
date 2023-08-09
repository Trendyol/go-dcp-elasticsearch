package helper

import (
	"io"
)

type BatchKeyData struct {
	Index int
	Size  int
}

type BatchKeyDataReader struct {
	s                 []byte
	b                 []BatchKeyData
	batchKeyDataIndex int
	batchKeyDataLen   int
	bufferIndex       int
	bufferSize        int
}

func (r *BatchKeyDataReader) Read(b []byte) (n int, err error) {
	if r.batchKeyDataIndex >= r.batchKeyDataLen {
		return 0, io.EOF
	}

	if r.bufferIndex >= len(r.s) {
		return 0, io.EOF
	}

	s := len(b)
	if s > r.bufferSize {
		s = r.bufferSize
	}

	n = copy(b, r.s[r.bufferIndex:r.bufferIndex+s])
	r.bufferSize -= n

	if r.bufferSize == 0 {
		r.batchKeyDataIndex++
		r.bufferIndex = getBatchDataIndex(r.b, r.batchKeyDataIndex)
		size := getBatchDataSize(r.b, r.batchKeyDataIndex)
		r.bufferSize = size
	} else {
		r.bufferIndex += n
	}

	return
}

func getBatchDataSize(b []BatchKeyData, index int) int {
	if index >= len(b) {
		return 0
	}

	return b[index].Size
}

func getBatchDataIndex(b []BatchKeyData, index int) int {
	if index >= len(b) {
		return 0
	}

	return b[index].Index
}

func (r *BatchKeyDataReader) Reset(s []byte, b []BatchKeyData) {
	*r = BatchKeyDataReader{s, b, 0, len(b), getBatchDataIndex(b, 0), getBatchDataSize(b, 0)}
}

func NewBatchKeyDataReader(s []byte, b []BatchKeyData) *BatchKeyDataReader {
	return &BatchKeyDataReader{s, b, 0, len(b), getBatchDataIndex(b, 0), getBatchDataSize(b, 0)}
}
