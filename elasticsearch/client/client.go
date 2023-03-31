package client

import (
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"
	"math"

	"github.com/elastic/go-elasticsearch/v7"
)

func NewElasticClient(elasticConfig *config.Elasticsearch) (*elasticsearch.Client, error) {
	config := elasticsearch.Config{
		MaxRetries:           math.MaxInt,
		Addresses:            elasticConfig.Urls,
		DiscoverNodesOnStart: true,
		Transport:            &Transport{},
	}
	es, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, err
	}
	return es, nil
}
