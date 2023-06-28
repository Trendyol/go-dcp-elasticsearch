package main

import (
	dcpcf "github.com/Trendyol/go-dcp-client/config"
	dcpes "github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
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
	connector, err := dcpes.NewConnectorBuilder(config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{
				"_default": "indexname",
			},
			Urls: []string{"http://localhost:9200"},
		},
		Dcp: dcpcf.Dcp{
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Hosts:      []string{"localhost:8091"},
			Dcp: dcpcf.ExternalDcp{
				Group: dcpcf.DCPGroup{
					Name: "groupName",
					Membership: dcpcf.DCPGroupMembership{
						Type: "static",
					},
				},
			},
			Metadata: dcpcf.Metadata{
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
