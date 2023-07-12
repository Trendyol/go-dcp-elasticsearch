package dcpelasticsearch

import (
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
)

type Mapper func(event couchbase.Event) []elasticsearch.ActionDocument

func DefaultMapper(event couchbase.Event) []elasticsearch.ActionDocument {
	if event.IsMutated {
		return []elasticsearch.ActionDocument{elasticsearch.NewIndexAction(event.Key, event.Value, nil)}
	}
	return []elasticsearch.ActionDocument{elasticsearch.NewDeleteAction(event.Key, nil)}
}
