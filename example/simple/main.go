package main

import (
	"github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
)

func mapper(event couchbase.Event) []elasticsearch.ActionDocument {
	if event.IsMutated {
		e := elasticsearch.NewIndexAction(event.Key, event.Value, nil)
		return []elasticsearch.ActionDocument{e}
	}
	e := elasticsearch.NewDeleteAction(event.Key, nil)
	return []elasticsearch.ActionDocument{e}
}

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
