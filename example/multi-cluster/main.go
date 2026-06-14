package main

import (
	"github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	dcpConfig "github.com/Trendyol/go-dcp/config"
)

// Route mutations to a named cluster when CollectionName matches; otherwise use the default cluster.
func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		doc := document.NewIndexAction(event.Key, event.Value, nil)
		if event.CollectionName == "orders" {
			doc.ClusterKey = "analytics"
		}
		return []document.ESActionDocument{doc}
	}
	del := document.NewDeleteAction(event.Key, nil)
	if event.CollectionName == "orders" {
		del.ClusterKey = "analytics"
	}
	return []document.ESActionDocument{del}
}

func main() {
	connector, err := dcpelasticsearch.NewConnectorBuilder(config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{
				"_default": "primary-index",
				"orders":   "primary-index",
			},
			Urls: []string{"http://localhost:9200"},
			Clusters: map[string]config.Elasticsearch{
				"analytics": {
					Urls: []string{"http://localhost:9201"},
					CollectionIndexMapping: map[string]string{
						"_default": "analytics-index",
						"orders":   "orders-analytics",
					},
				},
			},
		},
		Dcp: dcpConfig.Dcp{
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Hosts:      []string{"localhost:8091"},
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
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
