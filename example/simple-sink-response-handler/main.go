package main

import (
	"fmt"
	"github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		e := document.NewIndexAction(event.Key, event.Value, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetSinkResponseHandler(&SinkResponseHandler{}).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

type SinkResponseHandler struct {
}

func (crh *SinkResponseHandler) OnSuccess(ctx *elasticsearch.SinkResponseHandlerContext) {
	fmt.Printf("OnSuccess %v\n", string(ctx.Action.ID))
}

func (crh *SinkResponseHandler) OnError(ctx *elasticsearch.SinkResponseHandlerContext) {
	fmt.Printf("OnError %v\n", ctx.Err.Error())
}
