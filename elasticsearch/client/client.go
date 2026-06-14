package client

import (
	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp-elasticsearch/config"

	"github.com/elastic/go-elasticsearch/v7"
)

func NewElasticClient(cfg *config.Config) (*elasticsearch.Client, error) {
	return NewElasticClientFromElasticsearch(&cfg.Elasticsearch)
}

func NewElasticClientFromElasticsearch(es *config.Elasticsearch) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:              es.Username,
		Password:              es.Password,
		MaxRetries:            es.MaxRetries,
		Addresses:             es.Urls,
		Transport:             newTransport(*es),
		CompressRequestBody:   es.CompressionEnabled,
		DiscoverNodesOnStart:  !es.DisableDiscoverNodesOnStart,
		DiscoverNodesInterval: *es.DiscoverNodesInterval,
		Logger:                &LoggerAdapter{Logger: logger.Log},
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}
