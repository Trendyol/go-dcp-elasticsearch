package main

import (
	dcpelasticsearch "github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	dcpConfig "github.com/Trendyol/go-dcp/config"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		script := []byte(`{"source":"if (ctx._source == null) { ctx._source = params.doc } else { if (ctx._source.counter == null) { ctx._source.counter = 1 } else { ctx._source.counter += params.count } }","lang":"painless","params":{"count":1,"doc":{"counter":1}}}`)
		e := document.NewScriptUpdateAction(event.Key, script, nil)
		return []document.ESActionDocument{e}
	}
	e := document.NewDeleteAction(event.Key, nil)
	return []document.ESActionDocument{e}
}

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder(config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{
				"_default": "indexname",
			},
			Urls: []string{"http://localhost:9200"},
		},
		Dcp: dcpConfig.Dcp{
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Hosts:      []string{"localhost:8091"},
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpConfig.DCPGroupMembership{
						Type: "static",
					},
				},
			},
			Metadata: dcpConfig.Metadata{
				Config: map[string]string{
					"bucket":     "checkpoint-bucket-name",
					"scope":      "_default",
					"collection": "_default",
				},
				Type: "couchbase",
			},
		},
	}).
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
