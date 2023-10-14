package config

import (
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/elastic/go-elasticsearch/v7/estransport"
)

type Elasticsearch struct {
	CollectionIndexMapping  map[string]string                                                                `yaml:"collectionIndexMapping"`
	MaxConnsPerHost         *int                                                                             `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration     *time.Duration                                                                   `yaml:"maxIdleConnDuration"`
	TypeName                string                                                                           `yaml:"typeName"`
	Urls                    []string                                                                         `yaml:"urls"`
	BatchSizeLimit          int                                                                              `yaml:"batchSizeLimit"`
	BatchByteSizeLimit      int                                                                              `yaml:"batchByteSizeLimit"`
	BatchTickerDuration     time.Duration                                                                    `yaml:"batchTickerDuration"`
	ConcurrentRequest       int                                                                              `yaml:"concurrentRequest"`
	CompressionEnabled      bool                                                                             `yaml:"compressionEnabled"`
	Username                string                                                                           `yaml:"username"`
	Password                string                                                                           `yaml:"password"`
	CloudID                 string                                                                           `yaml:"cloudId"`
	APIKey                  string                                                                           `yaml:"apiKey"`
	ServiceToken            string                                                                           `yaml:"serviceToken"`
	CertificateFingerprint  string                                                                           `yaml:"certificateFingerprint"`
	Header                  map[string][]string                                                              `yaml:"header"`
	CACert                  []byte                                                                           `yaml:"caCert"`
	RetryOnStatus           []int                                                                            `yaml:"retryOnStatus"`
	DisableRetry            bool                                                                             `yaml:"disableRetry"`
	EnableRetryOnTimeout    bool                                                                             `yaml:"enableRetryOnTimeout"`
	DiscoverNodesOnStart    bool                                                                             `yaml:"discoverNodesOnStart"`
	DiscoverNodesInterval   time.Duration                                                                    `yaml:"discoverNodesInterval"`
	EnableMetrics           bool                                                                             `yaml:"enableMetrics"`
	EnableDebugLogger       bool                                                                             `yaml:"enableDebugLogger"`
	EnableCompatibilityMode bool                                                                             `yaml:"enableCompatibilityMode"`
	DisableMetaHeader       bool                                                                             `yaml:"disableMetaHeader"`
	UseResponseCheckOnly    bool                                                                             `yaml:"useResponseCheckOnly"`
	RetryBackoff            func(int) time.Duration                                                          `yaml:"retryBackoff"`
	Logger                  estransport.Logger                                                               `yaml:"logger"`
	Selector                estransport.Selector                                                             `yaml:"selector"`
	ConnectionPoolFunc      func([]*estransport.Connection, estransport.Selector) estransport.ConnectionPool `yaml:"connectionPoolFunc"`
}

type Config struct {
	Elasticsearch Elasticsearch `yaml:"elasticsearch"`
	Dcp           config.Dcp    `yaml:",inline"`
}

func (c *Config) ApplyDefaults() {
	if c.Elasticsearch.BatchTickerDuration == 0 {
		c.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}

	if c.Elasticsearch.BatchSizeLimit == 0 {
		c.Elasticsearch.BatchSizeLimit = 1000
	}

	if c.Elasticsearch.BatchByteSizeLimit == 0 {
		c.Elasticsearch.BatchByteSizeLimit = 10485760
	}

	if c.Elasticsearch.ConcurrentRequest == 0 {
		c.Elasticsearch.ConcurrentRequest = 1
	}
}
