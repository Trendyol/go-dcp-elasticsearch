package elasticsearch

import (
	"bytes"
	"context"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
)

type RejectionLogSinkResponseHandler struct {
	Config              *config.Config
	ElasticsearchClient *elasticsearch.Client
	Index               string
}

func (crh *RejectionLogSinkResponseHandler) OnInit(ctx *SinkResponseHandlerInitContext) {
	crh.ElasticsearchClient = ctx.ElasticsearchClient
	crh.Config = ctx.Config

	index := ctx.Config.Elasticsearch.RejectionLog.Index
	if index == "" {
		index = "cbes-rejects"
	}
	crh.Index = index

	if crh.checkNotIndicesExists() {
		_, err := esapi.IndicesCreateRequest{Index: index}.Do(context.Background(), crh.ElasticsearchClient)
		if err != nil {
			panic("Rejection Log Index Create Request Failed.")
		}
	}
}

func (crh *RejectionLogSinkResponseHandler) checkNotIndicesExists() bool {
	resp, err := esapi.IndicesExistsRequest{
		Index: []string{crh.Index},
	}.Do(context.Background(), crh.ElasticsearchClient)
	if err != nil {
		panic("Rejection Log Index Exist Request Failed.")
	}
	return resp.StatusCode == 404
}

func (crh *RejectionLogSinkResponseHandler) OnSuccess(_ *SinkResponseHandlerContext) {
}

func (crh *RejectionLogSinkResponseHandler) OnError(ctx *SinkResponseHandlerContext) {
	rejectionLog := RejectionLog{
		Index:      ctx.Action.IndexName,
		DocumentID: ctx.Action.ID,
		Action:     string(ctx.Action.Type),
		Error:      ctx.Err.Error(),
	}

	if crh.Config.Elasticsearch.RejectionLog.IncludeSource {
		rejectionLog.Source = string(ctx.Action.Source)
	}

	rejectionLogBytes, err := jsoniter.Marshal(rejectionLog)
	if err != nil {
		logger.Log.Error("Rejection Log marshal error, err: %v", err)
		return
	}

	req := esapi.IndexRequest{
		Index:   crh.Index,
		Body:    bytes.NewReader(rejectionLogBytes),
		Refresh: "false",
	}

	_, err = req.Do(context.Background(), crh.ElasticsearchClient)
	if err != nil {
		logger.Log.Error("Rejection Log write error, err: %v", err)
		panic(err)
	}
}

func NewRejectionLogSinkResponseHandler() SinkResponseHandler {
	return &RejectionLogSinkResponseHandler{}
}

type RejectionLog struct {
	Index      string
	Action     string
	Error      string
	Source     string
	DocumentID []byte
}
