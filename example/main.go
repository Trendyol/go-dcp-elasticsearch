package main

import (
	esCbConnector "go-elasticsearch-connect-couchbase"
	"go-elasticsearch-connect-couchbase/couchbase"
	"go-elasticsearch-connect-couchbase/elasticsearch/document"
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
	connector, err := esCbConnector.NewConnectorBuilder("./example/config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		return
	}
	defer connector.Close()
	connector.Start()
}
