package elasticsearch

import (
	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/elastic/go-elasticsearch/v7"
)

type SinkResponseHandlerContext struct {
	Action *document.ESActionDocument
	Err    error
}

type SinkResponseHandlerInitContext struct {
	Config              *config.Config
	ElasticsearchClient *elasticsearch.Client
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
	OnInit(ctx *SinkResponseHandlerInitContext)
}
