package config

import (
	"github.com/Trendyol/go-dcp/helpers"
	"time"

	"github.com/Trendyol/go-dcp/config"
)

type Elasticsearch struct {
	CollectionIndexMapping      map[string]string `yaml:"collectionIndexMapping"`
	MaxConnsPerHost             *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration         *time.Duration    `yaml:"maxIdleConnDuration"`
	DiscoverNodesInterval       *time.Duration    `yaml:"discoverNodesInterval"`
	TypeName                    string            `yaml:"typeName"`
	Urls                        []string          `yaml:"urls"`
	BatchSizeLimit              int               `yaml:"batchSizeLimit"`
	BatchByteSizeLimit          any               `yaml:"batchByteSizeLimit"`
	BatchTickerDuration         time.Duration     `yaml:"batchTickerDuration"`
	ConcurrentRequest           int               `yaml:"concurrentRequest"`
	CompressionEnabled          bool              `yaml:"compressionEnabled"`
	DisableDiscoverNodesOnStart bool              `yaml:"disableDiscoverNodesOnStart"`
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
		c.Elasticsearch.BatchByteSizeLimit = helpers.ResolveUnionIntOrStringValue("10mb")
	}

	if c.Elasticsearch.ConcurrentRequest == 0 {
		c.Elasticsearch.ConcurrentRequest = 1
	}

	if c.Elasticsearch.DiscoverNodesInterval == nil {
		duration := 5 * time.Minute
		c.Elasticsearch.DiscoverNodesInterval = &duration
	}
}
