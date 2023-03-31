package main

import (
	goelasticsearchconnectcouchbase "github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/document"
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
	connector, err := goelasticsearchconnectcouchbase.NewConnectorBuilder("./example/config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		return
	}
	defer connector.Close()
	connector.Start()
}
