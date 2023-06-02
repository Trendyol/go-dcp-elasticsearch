package config

import (
	"time"

	"github.com/Trendyol/go-dcp-client/config"
)

type Elasticsearch struct {
	CollectionIndexMapping map[string]string `yaml:"collectionIndexMapping"`
	MaxConnsPerHost        *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration    *time.Duration    `yaml:"maxIdleConnDuration"`
	TypeName               string            `yaml:"typeName"`
	Urls                   []string          `yaml:"urls"`
	BatchSizeLimit         int               `yaml:"batchSizeLimit"`
	BatchByteSizeLimit     int               `yaml:"batchByteSizeLimit"`
	BatchTickerDuration    time.Duration     `yaml:"batchTickerDuration"`
	CompressionEnabled     bool              `yaml:"compressionEnabled"`
}

type Config struct {
	Elasticsearch Elasticsearch `yaml:"elasticsearch"`
	Dcp           config.Dcp    `yaml:",inline"`
}

func (c *Config) ApplyDefaults() {
	if c.Elasticsearch.BatchTickerDuration == 0 {
		c.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}

	if c.Elasticsearch.TypeName == "" {
		c.Elasticsearch.TypeName = "_doc"
	}

	if c.Elasticsearch.BatchSizeLimit == 0 {
		c.Elasticsearch.BatchSizeLimit = 1000
	}

	if c.Elasticsearch.BatchByteSizeLimit == 0 {
		c.Elasticsearch.BatchByteSizeLimit = 10485760
	}
}
