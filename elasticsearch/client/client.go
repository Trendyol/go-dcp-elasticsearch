package client

import (
	"math"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"

	"github.com/elastic/go-elasticsearch/v7"
)

func NewElasticClient(config *config.Config) (*elasticsearch.Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		MaxRetries:           math.MaxInt,
		Addresses:            config.Elasticsearch.Urls,
		DiscoverNodesOnStart: true,
		Transport:            newTransport(config.Elasticsearch),
	})
	if err != nil {
		return nil, err
	}
	return es, nil
}
