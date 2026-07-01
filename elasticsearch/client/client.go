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
	cfg := elasticsearch.Config{
		Username:              es.Username,
		Password:              es.Password,
		MaxRetries:            es.MaxRetries,
		Addresses:             es.Urls,
		Transport:             newTransport(*es),
		CompressRequestBody:   es.CompressionEnabled,
		DiscoverNodesOnStart:  !es.DisableDiscoverNodesOnStart,
		DiscoverNodesInterval: *es.DiscoverNodesInterval,
		Logger:                &LoggerAdapter{Logger: logger.Log},
	}

	// When the bulk retry layer is enabled, disable the client's own retry so all
	// retry/backoff logic lives in one place. Otherwise the client retries 5xx and
	// transport errors with no backoff and a near-infinite MaxRetries (math.MaxInt),
	// shadowing the new layer's transport/whole-response branches entirely.
	if es.Retry != nil && es.Retry.Enabled {
		cfg.DisableRetry = true
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}
