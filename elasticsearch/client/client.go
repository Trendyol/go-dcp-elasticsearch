package client

import (
	"math"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"

	"github.com/elastic/go-elasticsearch/v7"
)

func NewElasticClient(config *config.Config) (*elasticsearch.Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:              config.Elasticsearch.Username,
		Password:              config.Elasticsearch.Password,
		MaxRetries:            math.MaxInt,
		Addresses:             config.Elasticsearch.Urls,
		Transport:             newTransport(config.Elasticsearch),
		CompressRequestBody:   config.Elasticsearch.CompressionEnabled,
		DiscoverNodesOnStart:  !config.Elasticsearch.DisableDiscoverNodesOnStart,
		DiscoverNodesInterval: *config.Elasticsearch.DiscoverNodesInterval,
		Logger:                &LoggerAdapter{Logger: logger.Log},
	})
	if err != nil {
		return nil, err
	}
	return es, nil
}
