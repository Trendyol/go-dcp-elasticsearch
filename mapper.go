package goelasticsearchconnectcouchbase

import (
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch/document"
)

type Mapper func(event couchbase.Event) []document.ESActionDocument

func DefaultMapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		return []document.ESActionDocument{document.NewIndexAction(event.Key, event.Value, nil)}
	}
	return []document.ESActionDocument{document.NewDeleteAction(event.Key, nil)}
}
