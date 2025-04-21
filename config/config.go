package config

import (
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/config"
)

type Elasticsearch struct {
	BatchByteSizeLimit          any               `yaml:"batchByteSizeLimit"`
	BatchCommitTickerDuration   *time.Duration    `yaml:"batchCommitTickerDuration"`
	CollectionIndexMapping      map[string]string `yaml:"collectionIndexMapping"`
	MaxConnsPerHost             *int              `yaml:"maxConnsPerHost"`
	MaxIdleConnDuration         *time.Duration    `yaml:"maxIdleConnDuration"`
	DiscoverNodesInterval       *time.Duration    `yaml:"discoverNodesInterval"`
	TypeName                    string            `yaml:"typeName"`
	Password                    string            `yaml:"password"`
	Username                    string            `yaml:"username"`
	RejectionLog                RejectionLog      `yaml:"rejectionLog"`
	Urls                        []string          `yaml:"urls"`
	BatchSizeLimit              int               `yaml:"batchSizeLimit"`
	BatchTickerDuration         time.Duration     `yaml:"batchTickerDuration"`
	ConcurrentRequest           int               `yaml:"concurrentRequest"`
	CompressionEnabled          bool              `yaml:"compressionEnabled"`
	DisableDiscoverNodesOnStart bool              `yaml:"disableDiscoverNodesOnStart"`
}

type RejectionLog struct {
	Index         string `yaml:"index"`
	IncludeSource bool   `yaml:"includeSource"`
}

type Config struct {
	Elasticsearch Elasticsearch `yaml:"elasticsearch" mapstructure:"elasticsearch"`
	Dcp           config.Dcp    `yaml:",inline" mapstructure:",squash"`
}

func (c *Config) ApplyDefaults() {
	if c.Elasticsearch.BatchTickerDuration == 0 {
		c.Elasticsearch.BatchTickerDuration = 10 * time.Second
	}

	if c.Elasticsearch.BatchSizeLimit == 0 {
		c.Elasticsearch.BatchSizeLimit = 1000
	}

	if c.Elasticsearch.BatchByteSizeLimit == nil {
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
