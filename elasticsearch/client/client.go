package client

import (
	"github.com/Trendyol/go-dcp/logger"
	"github.com/elastic/go-elasticsearch/v7"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
)

func NewElasticClient(cfg *config.Config) (*elasticsearch.Client, error) {
	return NewElasticClientFromElasticsearch(&cfg.Elasticsearch)
}

func NewElasticClientFromElasticsearch(es *config.Elasticsearch) (*elasticsearch.Client, error) {
	esTransport, err := newTransport(*es)
	if err != nil {
		return nil, err
	}

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:              es.Username,
		Password:              es.Password,
		MaxRetries:            es.MaxRetries,
		Addresses:             es.Urls,
		Transport:             esTransport,
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
