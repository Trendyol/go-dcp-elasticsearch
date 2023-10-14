package client

import (
	"math"

	"github.com/Trendyol/go-dcp-elasticsearch/config"

	"github.com/elastic/go-elasticsearch/v7"
)

func NewElasticClient(config *config.Config) (*elasticsearch.Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		MaxRetries:              math.MaxInt,
		Addresses:               config.Elasticsearch.Urls,
		Transport:               newTransport(config.Elasticsearch),
		CompressRequestBody:     config.Elasticsearch.CompressionEnabled,
		Username:                config.Elasticsearch.Username,
		Password:                config.Elasticsearch.Password,
		CloudID:                 config.Elasticsearch.CloudID,
		APIKey:                  config.Elasticsearch.APIKey,
		ServiceToken:            config.Elasticsearch.ServiceToken,
		CertificateFingerprint:  config.Elasticsearch.CertificateFingerprint,
		Header:                  config.Elasticsearch.Header,
		CACert:                  config.Elasticsearch.CACert,
		RetryOnStatus:           config.Elasticsearch.RetryOnStatus,
		DisableRetry:            config.Elasticsearch.DisableRetry,
		EnableRetryOnTimeout:    config.Elasticsearch.EnableRetryOnTimeout,
		DiscoverNodesOnStart:    config.Elasticsearch.DiscoverNodesOnStart,
		DiscoverNodesInterval:   config.Elasticsearch.DiscoverNodesInterval,
		EnableMetrics:           config.Elasticsearch.EnableMetrics,
		EnableDebugLogger:       config.Elasticsearch.EnableDebugLogger,
		EnableCompatibilityMode: config.Elasticsearch.EnableCompatibilityMode,
		DisableMetaHeader:       config.Elasticsearch.DisableMetaHeader,
		UseResponseCheckOnly:    config.Elasticsearch.UseResponseCheckOnly,
		RetryBackoff:            config.Elasticsearch.RetryBackoff,
		Logger:                  config.Elasticsearch.Logger,
		Selector:                config.Elasticsearch.Selector,
		ConnectionPoolFunc:      config.Elasticsearch.ConnectionPoolFunc,
	})
	if err != nil {
		return nil, err
	}
	return es, nil
}
