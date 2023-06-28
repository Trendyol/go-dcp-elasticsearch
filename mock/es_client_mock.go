package mock

import "bytes"

type EsClient struct {
	BulkFn       func(reader *bytes.Reader) error
	BulkFnCalled bool
}

func NewMockEsClient() *EsClient {
	return &EsClient{}
}

func (e *EsClient) Bulk(reader *bytes.Reader) error {
	e.BulkFnCalled = true
	return e.BulkFn(reader)
}

func (e *EsClient) OnBulk(bulkFn func(reader *bytes.Reader) error) {
	e.BulkFn = bulkFn
}
