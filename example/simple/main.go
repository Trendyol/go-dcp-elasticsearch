package main

import (
	dcpes "github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch"
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
	connector, err := dcpes.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
