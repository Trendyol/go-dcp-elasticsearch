package elasticsearch

import "github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"

type SinkResponseHandlerContext struct {
	Action *document.ESActionDocument
	Err    error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}
